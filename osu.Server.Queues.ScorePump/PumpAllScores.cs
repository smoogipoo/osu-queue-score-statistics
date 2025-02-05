// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using System.Threading;
using Dapper;
using McMaster.Extensions.CommandLineUtils;
using osu.Server.Queues.ScoreStatisticsProcessor.Models;

namespace osu.Server.Queues.ScorePump
{
    [Command("all", Description = "Pumps all completed scores")]
    public class PumpAllScores : ScorePump
    {
        [Option("--start_id")]
        public long StartId { get; set; }

        [Option("--sql", Description = "Specify a custom query to limit the scope of pumping")]
        public string? CustomQuery { get; set; }

        public int OnExecute(CancellationToken cancellationToken)
        {
            using (var dbMainQuery = Queue.GetDatabaseConnection())
            using (var db = Queue.GetDatabaseConnection())
            {
                string query = "SELECT * FROM solo_scores WHERE id >= @StartId";

                if (!string.IsNullOrEmpty(CustomQuery))
                    query += $" AND {CustomQuery}";

                Console.WriteLine($"Querying with \"{query}\"");
                var scores = dbMainQuery.Query<SoloScore>(query, this, buffered: false);

                foreach (var score in scores)
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    // attach any previous processing information
                    var history = db.QuerySingleOrDefault<ProcessHistory>("SELECT * FROM solo_scores_process_history WHERE id = @id", score);

                    Console.WriteLine($"Pumping {score}");
                    Queue.PushToQueue(new ScoreItem(score, history));
                }
            }

            return 0;
        }
    }
}
