using System;
using System.Globalization;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using CompanyService.WebAPI;
using CompanyService.WebAPI.SeedData;
using FluentAssertions;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Newtonsoft.Json;
using Xunit;

namespace CompanyService.Tests
{
    public class IntegrationTests
    {
        private TestServer _server;

        public HttpClient Client { get; private set; }

        public IntegrationTests()
        {
            SetUpClient();
        }

        private async Task CheckOnBadRequest(CompanyForm company, string errorMessage)
        {
            var response0 = await Client.PostAsync($"/api/companies",
                new StringContent(JsonConvert.SerializeObject(company), Encoding.UTF8, "application/json"));
            response0.StatusCode.Should().BeEquivalentTo(StatusCodes.Status400BadRequest);
            var responseMessage = await response0.Content.ReadAsStringAsync();
            bool result = responseMessage.Contains(errorMessage);
            responseMessage.Contains(errorMessage).Should().BeTrue();
        }

        [Fact]
        // Checking CompanyName
        public async Task Test1()
        {
            string titleError = "CompanyName is invalid: CompanyName must contain a minimum of 5 characters and a maximum of 35, and it must start with 'Company Name:'";
            await CheckOnBadRequest(new CompanyForm
            {
                CompanyName = "Micr",
                NumberOfEmployees = 534,
                AverageSalary = 187000
            }, titleError);
            await CheckOnBadRequest(new CompanyForm
            {
                CompanyName = "",
                NumberOfEmployees = 534,
                AverageSalary = 187000
            }, titleError);
            await CheckOnBadRequest(new CompanyForm
            {
                CompanyName = "microsoft",
                NumberOfEmployees = 534,
                AverageSalary = 187000
            }, titleError);
            await CheckOnBadRequest(new CompanyForm
            {
                CompanyName = "MicrosoftMicrosoftMicrosoftMicrosoft",
                NumberOfEmployees = 534,
                AverageSalary = 187000
            }, titleError);
            await CheckOnBadRequest(new CompanyForm
            {
                CompanyName = "Microsoft",
                NumberOfEmployees = 534,
                AverageSalary = 187000
            }, titleError);


            var company = new CompanyForm
            {
                CompanyName = "Company Name: Microsoft",
                NumberOfEmployees = 534,
                AverageSalary = 187000
            };
            var response3 = await Client.PostAsync($"/api/companies",
                new StringContent(JsonConvert.SerializeObject(company), Encoding.UTF8, "application/json"));
            response3.StatusCode.Should().BeEquivalentTo(StatusCodes.Status200OK);
        }


        [Fact]
        // Checking NumberOfEmployees
        public async Task Test2()
        {
            string errorMessage = "NumberOfEmployees is invalid: NumberOfEmployees must be greater than 1";
            await CheckOnBadRequest(new CompanyForm
            {
                CompanyName = "Company Name: Microsoft",
                NumberOfEmployees = 0,
                AverageSalary = 187000
            }, errorMessage);
            await CheckOnBadRequest(new CompanyForm
            {
                CompanyName = "Company Name: Microsoft",
                NumberOfEmployees = 1,
                AverageSalary = 187000
            }, errorMessage);
            await CheckOnBadRequest(new CompanyForm
            {
                CompanyName = "Company Name: Microsoft",
                NumberOfEmployees = -50,
                AverageSalary = 187000
            }, errorMessage);


            var company = new CompanyForm
            {
                CompanyName = "Company Name: Microsoft",
                NumberOfEmployees = 534,
                AverageSalary = 187000
            };
            var response3 = await Client.PostAsync($"/api/companies",
                new StringContent(JsonConvert.SerializeObject(company), Encoding.UTF8, "application/json"));
            response3.StatusCode.Should().BeEquivalentTo(StatusCodes.Status200OK);
        }


        [Fact]
        // Checking AverageSalary
        public async Task Test3()
        {
            string errorMessage = "AverageSalary is invalid: AverageSalary must be greater than 0";
            await CheckOnBadRequest(new CompanyForm
            {
                CompanyName = "Company Name: Microsoft",
                NumberOfEmployees = 534,
                AverageSalary = -50
            }, errorMessage);
            await CheckOnBadRequest(new CompanyForm
            {
                CompanyName = "Company Name: Microsoft",
                NumberOfEmployees = 534,
                AverageSalary = 0
            }, errorMessage);

            var company = new CompanyForm
            {
                CompanyName = "Company Name: Microsoft",
                NumberOfEmployees = 534,
                AverageSalary = 187000
            };
            var response3 = await Client.PostAsync($"/api/companies",
                new StringContent(JsonConvert.SerializeObject(company), Encoding.UTF8, "application/json"));
            response3.StatusCode.Should().BeEquivalentTo(StatusCodes.Status200OK);
        }

        private void SetUpClient()
        {
            var builder = new WebHostBuilder()
                .UseStartup<Startup>()
                .ConfigureServices(services =>
                {

                });

            _server = new TestServer(builder);

            Client = _server.CreateClient();
        }
    }
}
