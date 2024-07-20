using MountyHall.API.Models;
using MountyHall.API.Models.Enums;
using MountyHall.API.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MountHall.Tests
{
    public class MountyHallServiceTests
    {
        private readonly MountyHallService _service = new MountyHallService();

        [Fact]
        public void PlaySimulation_ShouldReturnTwoThirdProbabilityWinWhenSwitch()
        {
            // Arrange
            int initialPickedDoor = 1;
            ChoosingStrategy strategy = ChoosingStrategy.SWITCH;
            int noOfSimulations = 1000000;

            // Act
            var result = _service.PlaySimulation(initialPickedDoor, strategy, noOfSimulations);
            double expectedWinCountPercent = 66.67; // theoretical win probability
            double expectedLooseCountPercent = 100 - expectedWinCountPercent;
            double tolerence = 1.0;

            // Assert
            Assert.Equal(noOfSimulations, result.winCount + result.looseCount);
            Assert.InRange(result.winCountPercent, expectedWinCountPercent - tolerence, expectedWinCountPercent + tolerence);
            Assert.InRange(result.looseCountPercent, expectedLooseCountPercent - tolerence, expectedLooseCountPercent + tolerence);
        }

        [Fact]
        public void PlaySimulation_ShouldReturnOneThirdProbabilityWinWhenStay()
        {
            // Arrange
            int initialPickedDoor = 1;
            ChoosingStrategy strategy = ChoosingStrategy.STAY;
            int noOfSimulations = 1000000;

            // Act
            var result = _service.PlaySimulation(initialPickedDoor, strategy, noOfSimulations);
            double expectedWinCountPercent = 33.33; // theoretical win probability
            double expectedLooseCountPercent = 100 - expectedWinCountPercent;
            double tolerence = 1.0;

            // Assert
            Assert.Equal(noOfSimulations, result.winCount + result.looseCount);
            Assert.InRange(result.winCountPercent, expectedWinCountPercent - tolerence, expectedWinCountPercent + tolerence);
            Assert.InRange(result.looseCountPercent, expectedLooseCountPercent - tolerence, expectedLooseCountPercent + tolerence);
        }
    }
}
