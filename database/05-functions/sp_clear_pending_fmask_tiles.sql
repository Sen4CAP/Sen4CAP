create or replace function sp_clear_pending_fmask_tiles(
    _node_id text
)
returns void
as
$$
begin
    delete
    from fmask_history
    where status_id = 1 -- processing
      and node_id = _node_id;
end;
$$ language plpgsql volatile;
