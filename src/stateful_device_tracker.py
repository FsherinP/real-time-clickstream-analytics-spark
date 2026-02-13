from pyspark.sql.streaming import GroupState

def update_device_state(user_id, values, state: GroupState):

    if state.exists:
        devices = state.get()
    else:
        devices = set()

    for row in values:
        devices.add(row.context_did)

    state.update(devices)

    if len(devices) > 1:
        return [(user_id, len(devices))]

    return []
