import React, { useEffect } from "react";
import "./LoginAttemptList.css";

const LoginAttempt = (props) => <li {...props}>{props.children}</li>;

const LoginAttemptList = (props) => (
	<div className="Attempt-List-Main">
	 	<p>Recent activity</p>
	  	<input type="input" placeholder="Filter..." onChange={x=> props.onFilter(x.target.value)}/>
		<ul className="Attempt-List">
			{
				props.attempts &&
				props.attempts?.map((attempt,index)=>{
					return <LoginAttempt key={index}>{attempt.name}</LoginAttempt>;
				})
			}
		</ul>
	</div>
);

export default LoginAttemptList;