import React from 'react';

export default function MountyHallSimulation() {
    const [result, setResult] = React.useState({
      noOfWins: 0,
      noOfLooses: 0,
      winPercent: 0,
      loosePercent: 0,
    });
    const [initialDoor, setInitalDoor] = React.useState(1);
    const [strategy, setStrategy] = React.useState('SWITCH');
    const [noOfSimulations, setNoOfSimulations] = React.useState(1);

    const runSumulation = (event) => {
      event.preventDefault();
      fetch(
        `/api/Mountyhall?InitialPickDoor=${initialDoor}&Strategy=${strategy}&NoOfSimulations=${noOfSimulations}`,
        {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
          },
        }
      )
      .then(res=>res.json())
      .then((data) => {
         setResult(s=>({...data}));
      })
      .catch(err=>{
        console.log(err);
      });
    };
  
    return (
      <div>
        <form onSubmit={runSumulation}>
          <div>
            <label>Number of simulations: </label>
            <input type="number" max='1000000' value={noOfSimulations} onChange={e=>setNoOfSimulations(e.target.value)}/>
          </div>
          <div>
            <label>Pick the door: </label>
            <select name="door" value={initialDoor} onChange={e=>setInitalDoor(e.target.value)}>
              <option value="1">1</option>
              <option value="2">2</option>
              <option value="3">3</option>
            </select>
          </div>
          <div>
            <label>Strategy: </label>
            <select name="strategy" id="strategy" value={strategy} onChange={e=>setStrategy(e.target.value)}>
              <option value="STAY">STAY</option>
              <option value="SWITCH">SWITCH</option>
            </select>
          </div>
          <input type="submit" value="Run Simulation" />
        </form>

        <div>
          <h2>Number Of Wins: {result.noOfWins}</h2>
          <h2>Number Of Looses: {result.noOfLooses}</h2>
          <h2>Wins %: {result.winPercent}</h2>
          <h2>Loose %: {result.loosePercent}</h2>
        </div>
      </div>
    );
  }
  