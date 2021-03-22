create or replace function sp_clear_pending_l1_tiles(
    _node_id text
)
returns void
as
$$
begin
    if (select current_setting('transaction_isolation') not ilike 'serializable') then
        raise exception 'Please set the transaction isolation level to serializable.' using errcode = 'UE001';
    end if;

    delete
    from l1_tile_history
    using downloader_history
    where downloader_history.id = l1_tile_history.downloader_history_id
      and l1_tile_history.status_id = 1 -- processing
      and l1_tile_history.node_id = _node_id
      and downloader_history.satellite_id in (1, 2); -- sentinel2, landsat8

    update downloader_history
    set status_id = 2 -- downloaded
    where status_id = 7 -- processing
      and not exists (
        select *
        from l1_tile_history
        where status_id = 1 -- processing
    );
end;
$$ language plpgsql volatile;
