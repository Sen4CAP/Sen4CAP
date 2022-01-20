insert into default_scheduled_tasks
values (2, 'L3A', 2, 0, 31, 'start', 'month', '1 month - 1 day', 60, 1, null),
       (3, 'L3B', 1, 1, 0, 'start', null, '1 day', 60, 1, null),
       (5, 'L4A', 2, 0, 31, 'mid', null, null, 60, 1, null),
       (6, 'L4B', 2, 0, 31, 'mid', null, null, 60, 1, null),
       (9, 'S4C_L4A', 2, 0, 31, 'mid', null, null, 60, 1, null),
       (10, 'S4C_L4B', 2, 0, 31, 'start', null, '31 days', 60, 1, null),
       (11, 'S4C_L4C', 1, 7, 0, 'start', null, '7 days', 60, 1, null),
       (14, 'S4C_MDB1', 1, 1, 0, 'start', null, '1 day', 60, 1, null),
       (22, 'L3COMP', 2, 0, 31, 'start', 'month', '1 month - 1 day', 60, 1, null);
