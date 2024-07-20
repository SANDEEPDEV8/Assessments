import React, { useEffect, useState } from "react";
import './App.css';
import LoginForm from './LoginForm';
import LoginAttemptList from './LoginAttemptList';

const App = () => {
  const [loginAttempts, setLoginAttempts] = useState([]);
  const [filteredAttempts, setFilteredAttempts] = useState([]);

  const addAttempt =(name, password) => {
    setLoginAttempts((attempts) => {
      const newAttempts = [...attempts, { name, password }];
      return newAttempts; 
    });
  } 

  useEffect(()=>{
    setFilteredAttempts(loginAttempts);
  },[loginAttempts])

  const handleFilter = filterValue =>{
    if(filterValue){
      const attempts = loginAttempts.filter(obj => obj.name.includes(filterValue));
      setFilteredAttempts(attempts);
    }
    else{
      setFilteredAttempts(loginAttempts);
    }
  }

  return (
    <div className="App">
      <LoginForm onSubmit={({ name, password }) => addAttempt(name, password)} />
      <LoginAttemptList attempts={filteredAttempts} onFilter={value=>handleFilter(value)}/>
    </div>
  );
};

export default App;