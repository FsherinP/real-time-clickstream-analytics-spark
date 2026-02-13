from pyspark.sql.streaming import GroupState, GroupStateTimeout

def update_funnel_state(key, values, state: GroupState):

    user_id, content_id = key

    if state.exists:
        current_state = state.get()
    else:
        current_state = {
            "impression": False,
            "started": False,
            "completed": False
        }

    for row in values:
        if row.eid == "IMPRESSION":
            current_state["impression"] = True
        elif row.eid == "START":
            current_state["started"] = True
        elif row.eid == "END":
            current_state["completed"] = True

    state.update(current_state)
    state.setTimeoutDuration("30 minutes")

    if state.hasTimedOut:
        result = (
            user_id,
            content_id,
            current_state["impression"],
            current_state["started"],
            current_state["completed"]
        )
        state.remove()
        return [result]

    return []
