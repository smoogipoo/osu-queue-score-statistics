// Copyright (c) ppy Pty Ltd <contact@ppy.sh>. Licensed under the MIT Licence.
// See the LICENCE file in the repository root for full licence text.

using System;
using Dapper;
using JetBrains.Annotations;
using MySqlConnector;
using osu.Server.Queues.ScoreStatisticsProcessor.Models;

namespace osu.Server.Queues.ScoreStatisticsProcessor.Processors
{
    /// <summary>
    /// Increment total user play time.
    /// </summary>
    [UsedImplicitly]
    public class PlayTimeProcessor : IProcessor
    {
        public void RevertFromUserStats(SoloScore score, UserStats userStats, int previousVersion, MySqlConnection conn, MySqlTransaction transaction)
        {
            if (previousVersion >= 2)
                userStats.total_seconds_played -= getPlayLength(score, conn);
        }

        public void ApplyToUserStats(SoloScore score, UserStats userStats, MySqlConnection conn, MySqlTransaction transaction)
        {
            if (score.ended_at == null)
                throw new InvalidOperationException("Attempting to increment play time when score was never finished.");

            userStats.total_seconds_played += getPlayLength(score, conn);
        }

        public void ApplyGlobal(SoloScore score, MySqlConnection conn)
        {
        }

        private static int getPlayLength(SoloScore score, MySqlConnection conn)
        {
            int drainLengthSeconds = conn.QueryFirstOrDefault("SELECT drain_length FROM osu_beatmaps WHERE beatmap_id = @beatmap_id", score);

            // TODO: calculate the play rate from mods...

            // TODO: make sure we have a fail/retry time

            return drainLengthSeconds;
        }
    }
}
