using System;
using System.ComponentModel.DataAnnotations;

namespace CompanyService.WebAPI.Models
{
    public class Company
    {
        [MinLength(5,ErrorMessage = "CompanyName is invalid: CompanyName must contain a minimum of 5 characters and a maximum of 35, and it must start with 'Company Name:'")]
        [MaxLength(35, ErrorMessage = "CompanyName is invalid: CompanyName must contain a minimum of 5 characters and a maximum of 35, and it must start with 'Company Name:'")]
        [RegularExpression(@"^Company Name:.*$", ErrorMessage = "CompanyName is invalid: CompanyName must contain a minimum of 5 characters and a maximum of 35, and it must start with 'Company Name:'")]
        public string CompanyName { get; set; }

        [Range(2, Int32.MaxValue,ErrorMessage = "NumberOfEmployees is invalid: NumberOfEmployees must be greater than 1")]
        public int NumberOfEmployees { get; set; }

        [Required]
        [Range(1,Int32.MaxValue,ErrorMessage = "AverageSalary is invalid: AverageSalary must be greater than 0")]
        public int AverageSalary { get; set; }
    }
}