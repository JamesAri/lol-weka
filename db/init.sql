-- create table which will contain all the played matches
CREATE TABLE matches(
	id					SERIAL PRIMARY KEY,
	match_string		varchar(40) NOT NULL UNIQUE
)
