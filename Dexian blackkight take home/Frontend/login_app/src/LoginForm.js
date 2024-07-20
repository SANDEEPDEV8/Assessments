import React, { useState } from "react";
import './LoginForm.css';

const LoginForm = (props) => {
	const [name, setName] = useState('');
	const [password, setPassword] = useState('');

	const handleSubmit = (event) =>{
		event.preventDefault();

		props.onSubmit({
			name: name,
			password: password,
		});
		cleanInputControls();
	}

	const cleanInputControls= () =>{
		setName('');
		setPassword('');
	}

	const isDisabled = !name || !password;

	return (
		<form className="form">
			<h1>Login</h1>
			<label htmlFor="name">Name</label>
			<input type="text" id="name" name="name" value={name} onChange={x=>setName(x.target.value?.trim())}/>
			<label htmlFor="password">Password</label>
			<input type="password" id="password" name="password" value={password} onChange={x=>setPassword(x.target.value)}/>
			<button type="submit" onClick={handleSubmit} disabled={isDisabled}>Continue</button>
		</form>
	)
}

export default LoginForm;