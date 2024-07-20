using MountyHall.API.Models.Enums;

namespace MountyHall.API.Models
{
    public class Door
    {
        public Door(int index, Prize prize)
        {
            Index = index;
            Prize = prize;
        }

        public int Index { get; }
        public Prize Prize { get; set; }
    }
}
