from akita_genesis.utils.logger import log, get_recent_logs

log.info('TEST-LOG-ENTRY-123')
entries = get_recent_logs(limit=10)
print('found', len(entries))
print(entries[-1] if entries else None)
