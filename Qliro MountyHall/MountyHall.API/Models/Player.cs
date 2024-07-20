namespace MountyHall.API.Models
{
    public class Player
    {
        public int Id { get; }
        public Door PickedDoor { get; private set; }

        public Player(int id)
        {
            Id = id;
        }

        public void PickDoor(Door doorIndex)
        {
            PickedDoor = doorIndex;
        }
    }
}
