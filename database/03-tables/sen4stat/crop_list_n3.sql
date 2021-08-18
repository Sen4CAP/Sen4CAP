create table crop_list_n3(
    code_n3 int not null primary key,
    name text not null,
    code_n2 int not null references crop_list_n2(code_n2)
);
