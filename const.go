package dataio
import "time"

const DEBUG_MODE_LOG = false
const DEBUG_MODE_UNIT_CONSUMED_LOG = true
//const URL_dynamoDB = "us-west-2"
const URL_dynamoDB = "http://localhost:8000"
const TIMEOUT = 3 * time.Minute

const TABLE_NAME_COUNTER = "counter"
const TABLE_NAME_ACCOUNTS = "accounts"
const TABLE_NAME_USERS = "users"

const KEY_USER_ID = "uid"
