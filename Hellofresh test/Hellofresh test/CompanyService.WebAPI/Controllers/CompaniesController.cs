using System.Threading.Tasks;
using CompanyService.WebAPI.Models;
using Microsoft.AspNetCore.Mvc;

namespace CompanyService.WebAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]

    public class CompaniesController : ControllerBase
    {
        [HttpPost("")]
        public async Task<IActionResult> Validate(Company company)
        {
            return Ok();
        }
    }
}
