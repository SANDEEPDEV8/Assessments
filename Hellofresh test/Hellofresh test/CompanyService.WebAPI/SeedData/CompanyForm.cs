using System;
using Newtonsoft.Json;

namespace CompanyService.WebAPI.SeedData
{
    public class CompanyForm
    {
        [JsonProperty("companyName")]
        public string CompanyName { get; set; }

        [JsonProperty("numberOfEmployees")]
        public int NumberOfEmployees { get; set; }

        [JsonProperty("averageSalary")]
        public int AverageSalary { get; set; }
    }
}
