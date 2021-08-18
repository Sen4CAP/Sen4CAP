create table crop_list_n2(
    code_n2 int not null primary key,
    name text not null,
    code_n1 int not null references crop_list_n1(code_n1)
);
