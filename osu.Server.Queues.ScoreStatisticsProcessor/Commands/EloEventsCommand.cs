// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Dapper.Contrib.Extensions;
using McMaster.Extensions.CommandLineUtils;
using Newtonsoft.Json;
using osu.Game.Beatmaps.Legacy;
using osu.Game.Online.API;
using osu.Game.Rulesets;
using osu.Game.Rulesets.Mods;
using osu.Game.Rulesets.Osu;
using osu.Server.QueueProcessor;
using osu.Server.Queues.ScoreStatisticsProcessor.Elo;
using osu.Server.Queues.ScoreStatisticsProcessor.Models;
using osu.Server.Queues.ScoreStatisticsProcessor.Stores;

namespace osu.Server.Queues.ScoreStatisticsProcessor.Commands
{
    // Table structure:

    // CREATE TABLE user_elo_events
    // (
    //     ## The score ID.
    //     score_id        bigint PRIMARY KEY,
    //     ## The user ID.
    //     user_id         int,
    //
    //     ## The contest ID. Used for ordering.
    //     contest_id      int,
    //
    //     ## The Elo rating after completion of the score.
    //     elo_rating      int,
    //     ## The Elo stddev after completion of the score.
    //     elo_rating_dev  float,
    //     ## The Elo performance measured for the score.
    //     elo_performance int,
    //
    //     ## The beatmap star rating.
    //     star_rating     float,
    //     ## The score PP rating.
    //     pp              float
    // );
    // CREATE INDEX index_user_id ON user_elo_events (user_id);

    // All events for a single player.

    // SELECT e.contest_id, s.ended_at, e.elo_rating, e.elo_performance, e.star_rating, e.pp
    // FROM user_elo_events e
    //          JOIN scores s
    //               ON s.id = e.score_id
    // WHERE s.user_id = {{ userid }}
    // ORDER BY e.contest_id;

    // All Elo ratings and PP for players.

    // SELECT e.user_id, contest_id, elo_rating, s.rank_score
    // FROM (SELECT user_id,
    //              contest_id,
    //              elo_rating,
    //              ROW_NUMBER() OVER (
    //                  PARTITION BY user_id
    //                  ORDER BY contest_id DESC
    //                  ) AS rn
    //       FROM user_elo_events) e
    //          JOIN osu_user_stats s
    //               ON s.user_id = e.user_id
    // WHERE rn = 1
    // ORDER BY elo_rating DESC;

    [Command(Name = "elo-events")]
    public class EloEventsCommand
    {
        public async Task<int> OnExecuteAsync(CommandLineApplication app, CancellationToken cancellationToken)
        {
            EloSystem eloSystem = new EloSystem { MaxHistory = 10 };
            Dictionary<uint, EloPlayer> eloPlayers = new Dictionary<uint, EloPlayer>();
            List<EloEventEntry> pendingEvents = new List<EloEventEntry>();

            using var conn = await DatabaseAccess.GetConnectionAsync(cancellationToken);

            Console.WriteLine("Querying difficulties...");

            Dictionary<(uint beatmapId, LegacyMods mods), BeatmapDifficultyEntry> beatmapDifficulties = [];
            foreach (var entry in await conn.QueryAsync<BeatmapDifficultyEntry>("SELECT * FROM osu_beatmap_difficulty"))
                beatmapDifficulties[(entry.beatmap_id, (LegacyMods)entry.mods)] = entry;

            Console.WriteLine("Querying beatmaps...");

            // Retrieve all beatmaps.
            //
            // Assumption: Earlier beatmaps represent an earlier stage in each user's skill progression.
            //
            int[] beatmaps = (await conn.QueryAsync<int>(
                """
                SELECT b.beatmap_id
                FROM osu_beatmaps b
                JOIN osu_beatmapsets s
                    ON s.beatmapset_id = b.beatmapset_id
                WHERE b.beatmap_id IN
                (
                    SELECT DISTINCT(beatmap_id)
                    FROM scores
                )
                ORDER BY s.approved_date;
                """)).ToArray();

            List<Task> flushTasks = new List<Task>();
            int processedCount = 0;

            foreach (int beatmapId in beatmaps)
            {
                // Only allow beatmaps that are valid for ranking.
                Beatmap? beatmap = await BeatmapStore.GetBeatmapAsync((uint)beatmapId, conn);
                if (beatmap == null || !BeatmapStore.IsBeatmapValidForPerformance(beatmap, 0))
                    continue;

                // Retrieve all scores for the beatmap, ordered by PP.
                //
                // Assumption: Earlier beatmaps are played less often as skill increases, such that only the best scores should be considered.
                //             (in practice we don't have all scores ever for each user, there's nothing more that can be done anyway).
                //
                // Assumption: PP is a reasonable function of skill.
                //
                ScoreEntry[] contestScores = (await conn.QueryAsync<ScoreEntry>(
                    """
                    SELECT id, user_id, pp, data->'$.mods' AS 'mods'
                    FROM scores
                    WHERE beatmap_id = @BeatmapId
                        AND ruleset_id = 0
                    ORDER BY pp DESC;
                    """, new
                    {
                        BeatmapId = beatmapId
                    })).ToArray();

                // Filter to a single score for each user that maximises PP.
                contestScores = contestScores.GroupBy(s => s.user_id).Select(g => g.First()).ToArray();

                EloPlayer[] contestPlayers = new EloPlayer[contestScores.Length];

                for (int i = 0; i < contestScores.Length; i++)
                {
                    eloPlayers.TryAdd(contestScores[i].user_id, new EloPlayer());
                    contestPlayers[i] = eloPlayers[contestScores[i].user_id];
                }

                // Run the contest.
                //
                // Assumption: Scores may be set in any order/at any time, such that rating decay doesn't make sense.
                //
                eloSystem.RecordContest(new EloContest(DateTimeOffset.UnixEpoch, contestPlayers.ToArray()));

                for (int i = 0; i < contestScores.Length; i++)
                {
                    Ruleset ruleset = new OsuRuleset();
                    APIMod[] apiMods = JsonConvert.DeserializeObject<APIMod[]>(contestScores[i].mods) ?? [];
                    Mod[] mods = apiMods.Select(m => m.ToMod(ruleset)).ToArray();
                    LegacyMods legacyMods = BeatmapStore.GetLegacyModsForAttributeLookup(beatmap, ruleset, mods);

                    pendingEvents.Add(new EloEventEntry
                    {
                        score_id = contestScores[i].id,
                        user_id = contestScores[i].user_id,
                        contest_id = processedCount,
                        elo_rating = (int)Math.Round(contestPlayers[i].ApproximatePosterior.Mu),
                        elo_rating_dev = contestPlayers[i].ApproximatePosterior.Sig,
                        elo_performance = (int)Math.Round(contestPlayers[i].LastPerformance.Mu),
                        star_rating = beatmapDifficulties.TryGetValue(((uint)beatmapId, legacyMods), out var entry) ? entry.diff_unified : -1,
                        pp = contestScores[i].pp
                    });
                }

                if (processedCount % 1000 == 0)
                {
                    flushTasks.Add(flushEvents(pendingEvents.ToArray()));
                    pendingEvents.Clear();
                }

                Console.WriteLine($"Processed {++processedCount} / {beatmaps.Length}");
            }

            flushTasks.Add(flushEvents(pendingEvents.ToArray()));
            await Task.WhenAll(flushTasks.ToArray());

            return 0;
        }

        private async Task flushEvents(EloEventEntry[] events)
        {
            Console.WriteLine("Flushing events...");

            using var flushConnection = await DatabaseAccess.GetConnectionAsync(CancellationToken.None);
            await flushConnection.InsertAsync(events);
        }

        [Table("user_elo_events")]
        private class EloEventEntry
        {
            public ulong score_id { get; set; }
            public uint user_id { get; set; }

            public int contest_id { get; set; }

            public int elo_rating { get; set; }
            public double elo_rating_dev { get; set; }
            public int elo_performance { get; set; }

            public double star_rating { get; set; }
            public double pp { get; set; }
        }

        [Table("osu_beatmap_difficulty")]
        private class BeatmapDifficultyEntry
        {
            public uint beatmap_id { get; set; }
            public int mods { get; set; }
            public double diff_unified { get; set; }
        }

        private class ScoreEntry
        {
            public ulong id { get; set; }
            public uint user_id { get; set; }
            public double pp { get; set; }
            public string mods { get; set; }
        }
    }
}
