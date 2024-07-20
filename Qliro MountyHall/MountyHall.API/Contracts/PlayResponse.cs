using MountyHall.API.Contracts;

namespace MountyHall.API.Models.Contracts
{
    public class PlayResponse: BaseErrorResponse
    {
        public int NoOfWins { get; set; }
        public int NoOfLooses { get; set; }
        public double WinPercent { get; set; }
        public double LoosePercent { get; set; }
        public PlayResponse()
        {

        }

        public PlayResponse(int noOfWins, int noOfLooses, double winPercent, double loosePercent)
        {
            NoOfWins = noOfWins;
            NoOfLooses = noOfLooses;
            WinPercent = Math.Round(winPercent,2);
            LoosePercent = Math.Round(loosePercent, 2);
        }
    }
}
