create view v_site_config as
select config_metadata.key,
       site.id as site_id,
       config.value
from site
         cross join config_metadata
         cross join lateral (
    select coalesce((
                        select value
                        from config
                        where key = config_metadata.key
                          and config.site_id = site.id
                    ), (
                        select value
                        from config
                        where key = config_metadata.key
                          and config.site_id is null
                    )) as value
    ) config;
