package rdb

const Prefix = "fairq:v1:"
const Ready = Prefix + "groups:ready"
const Members = Prefix + "groups:members"
const Active = Prefix + "active"
const Lease = Prefix + "lease"
const Retry = Prefix + "retry"
const Archived = Prefix + "archived"
const Completed = Prefix + "completed"
const BadRefs = Prefix + "bad_refs"

var scriptKeys = []string{Ready, Members, Active, Lease, Retry, Archived, Completed, BadRefs}

func TaskKey(id string) string       { return Prefix + "task:" + id }
func PendingKey(group string) string { return Prefix + "pending:" + group }
