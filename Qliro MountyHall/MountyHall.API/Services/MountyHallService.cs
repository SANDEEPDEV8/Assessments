using MountyHall.API.Models;
using MountyHall.API.Models.Enums;
using System;

namespace MountyHall.API.Services
{
    public class MountyHallService : IMountyHallService
    {
        static readonly Random _random = new();

        public (int winCount, int looseCount, double winCountPercent, double looseCountPercent)
            PlaySimulation(int initialPickedDoor, ChoosingStrategy strategy, int noOfSimulations)
        {
            int winCount = 0;
            for (int i = 0; i < noOfSimulations; i++)
            {
                bool result = Play(initialPickedDoor, strategy);
                if (result)
                {
                    winCount++;
                }
            }

            int looseCount = noOfSimulations - winCount;
            double winCountPercent = ((double)winCount / (double)noOfSimulations) * 100;
            double looseCountPercent = ((double)looseCount / (double)noOfSimulations) * 100;

            return (winCount, looseCount, winCountPercent, looseCountPercent);
        }
   
        public bool Play(int initialPickDoor, ChoosingStrategy strategy)
        {

            var doors= new List<Door> {
                new Door(1, Prize.Goat),
                new Door(2, Prize.Goat),
                new Door(3, Prize.Goat)
            };
            //random car door
            var carDoorIndex = _random.Next(0, 3);
            doors[carDoorIndex].Prize = Prize.Car;
            Door carDoor = doors[carDoorIndex];

            //player picks the initial door
            var player = new Player(1);
            var pickedDoor = doors.Single(d => d.Index == initialPickDoor);
            player.PickDoor(pickedDoor);

            //remaining door is car door, if player picked door is goat
            Door remainingDoor = carDoor;

            //pick random if remaining doors are both goat doors
            if (pickedDoor.Index == carDoor.Index)
            {
                while (remainingDoor.Index == carDoor.Index)
                    remainingDoor = doors[_random.Next(0, 3)];
            }

            //result
            return strategy switch
            {
                ChoosingStrategy.STAY => carDoor.Index == player.PickedDoor.Index,
                ChoosingStrategy.SWITCH => carDoor.Index == remainingDoor.Index,
                _ => throw new ArgumentOutOfRangeException(nameof(strategy), $"Invalid Strategy: {strategy}"),
            };
        }

    }
}
