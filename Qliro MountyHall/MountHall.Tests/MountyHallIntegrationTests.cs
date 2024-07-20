using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using MountyHall.API;
using MountyHall.API.Models.Contracts;
using MountyHall.API.Models.Enums;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace MountHall.Tests
{
    public class MountyHallIntegrationTests: IClassFixture<WebApplicationFactory<Program>>
    {
        private readonly HttpClient _client;

        public MountyHallIntegrationTests(WebApplicationFactory<Program> factory)
        {
            var httpClientOptions = new WebApplicationFactoryClientOptions
            {
                BaseAddress = new Uri("https://localhost:7204"),
            };
            _client = factory.CreateClient(httpClientOptions);
        }

        [Theory]
        [InlineData(1, ChoosingStrategy.SWITCH, 1000000)]
        public async Task MountyhallController_Should_Return_OK_Status_Code_With_TwoThird_WinProb_OnSwitch(int initialDoor,ChoosingStrategy strategy,int noOfSimulations)
        {
            // Arrange
            var url = $"/api/Mountyhall?InitialPickDoor={initialDoor}&Strategy={strategy.ToString()}&NoOfSimulations={noOfSimulations}";

            // Act
            var response = await _client.GetAsync(url);
            double expectedWinCountPercent = 66.67;
            double tolerence = 1.0;

            // Assert
            response.EnsureSuccessStatusCode();
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var res=JsonConvert.DeserializeObject<PlayResponse>(await response.Content.ReadAsStringAsync());
            Assert.InRange(res.WinPercent, expectedWinCountPercent - tolerence, expectedWinCountPercent + tolerence);
        }

        [Theory]
        [InlineData(1, ChoosingStrategy.STAY, 1000000)]
        public async Task MountyhallController_Should_Return_OK_Status_Code_With_OneThird_WinProb_OnStay(int initialDoor, ChoosingStrategy strategy, int noOfSimulations)
        {
            // Arrange
            var url = $"/api/Mountyhall?InitialPickDoor={initialDoor}&Strategy={strategy.ToString()}&NoOfSimulations={noOfSimulations}";

            // Act
            var response = await _client.GetAsync(url);
            double expectedWinCountPercent = 33.33;
            double tolerence = 1.0;

            // Assert
            response.EnsureSuccessStatusCode();
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);

            var res = JsonConvert.DeserializeObject<PlayResponse>(await response.Content.ReadAsStringAsync());
            Assert.InRange(res.WinPercent, expectedWinCountPercent - tolerence, expectedWinCountPercent + tolerence);
        }
    }
}
