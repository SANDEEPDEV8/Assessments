## Environment:
- .NET version: 3.0

## Read-Only Files:
- CompanyService.Tests/IntegrationTests.cs
- CompanyService.WebAPI/Controllers/CompaniesController.cs

## Data:
Example of a company data JSON object:
```
{
   "companyName": "Company Name: Microsoft",
   "numberOfEmployees": 230,
   "averageSalary": 211000
}
```

## Requirements:

A company is launching a service that can validate a company model. The service should be a web API layer using .NET Core 3.0. You already have a prepared infrastructure and need to implement validation logic for the company model as per the guidelines below. Perform validation in models, not in controllers.

Each company data is a JSON object describing the details of the company. Each object has the following properties:

- companyName: the name of the company. [String]
- numberOfEmployees: the number of employees in the company. [Integer]
- averageSalary: the average salary of the employees in the company. [Integer]

The following API needs to be implemented.

`POST` request to  `api/companies`:

- The HTTP response code should be 200 on success.
- For the body of the request, please use the JSON example of the company model given above.
- If the company model is invalid, return status code 400. When you send 400, add an appropriate error message to the response as described below.

The company model should be validated based on following rules:

- The companyName field is required and must contain a minimum of 5 characters and a maximum of 35, and it must start with 'Company Name:'. If the field is invalid, add this error message: `"CompanyName is invalid: CompanyName must contain a minimum of 5 characters and a maximum of 35, and it must start with 'Company Name:'"`.
- The numberOfEmployees field is required and must be greater than 1. If the field is invalid, add this error message: `"NumberOfEmployees is invalid: NumberOfEmployees must be greater than 1"`.
- The averageSalary field is required and must be greater than 0. If the field is invalid, add this error message: `"AverageSalary is invalid: AverageSalary must be greater than 0"`.