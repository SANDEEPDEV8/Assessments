using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using MountyHall.API.Models.Contracts;
using MountyHall.API.Services;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace MountyHall.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class MountyhallController : ControllerBase
    {
        public IMountyHallService _mountyHallService { get; }
        public ILogger _logger { get; }

        public MountyhallController(IMountyHallService mountyHallService,ILogger<MountyhallController> logger)
        {
            _mountyHallService = mountyHallService;
            _logger = logger;
        }

        [HttpGet]
        public IActionResult Get([FromQuery] PlayRequest request)
        {
            try
            {
                var response = _mountyHallService.PlaySimulation(request.InitialPickDoor, request.Strategy, request.NoOfSimulations);
                return Ok(new PlayResponse(response.winCount,
                                            response.looseCount,
                                            response.winCountPercent,
                                            response.looseCountPercent));
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Mounty Hall Simulation Failed.");

                return StatusCode(500, new PlayResponse
                {
                    ErrorMessage = "Mounty Hall Simulation Failed.",
                });
            }
            
        }

    }
}
