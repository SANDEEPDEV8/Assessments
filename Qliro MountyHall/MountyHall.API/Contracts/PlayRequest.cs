using MountyHall.API.Models.Enums;
using System.ComponentModel.DataAnnotations;
using System.Runtime.InteropServices;

namespace MountyHall.API.Models.Contracts
{
    public class PlayRequest
    {
        [Required]
        [Range(1,3,ErrorMessage = "InitialPickDoor should be betweeen 1 and 3")]
        public int InitialPickDoor { get; set; }

        [Required]
        [Range(0, 1, ErrorMessage = "InitialPickDoor should be stay(0) or switch(1)")]
        public ChoosingStrategy Strategy { get; set; }

        [Range(1,1000000, ErrorMessage = "NoOfSimulations should not exceed 1000")]
        public int NoOfSimulations { get; set; }
    }
}
