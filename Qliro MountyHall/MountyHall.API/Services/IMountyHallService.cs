using MountyHall.API.Models.Enums;

namespace MountyHall.API.Services
{
    public interface IMountyHallService
    {
        (int winCount, int looseCount, double winCountPercent, double looseCountPercent) PlaySimulation(int initialPickedDoor, ChoosingStrategy strategy, int noOfSimulations);
        bool Play(int initialPickedDoor, ChoosingStrategy strategy);
    }
}