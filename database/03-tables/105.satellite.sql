CREATE TABLE satellite
(
  id int not null,
  satellite_name varchar NOT NULL,
  required BOOLEAN NOT NULL DEFAULT false,

  CONSTRAINT satellite_pkey PRIMARY KEY (id)
)
