package dataio
import "time"

const DEBUG_MODE = false
//const URL_dynamoDB = "us-west-2"
const URL_dynamoDB = "http://localhost:8000"
const TIMEOUT = 1 * time.Minute

const TABLE_NAME_COUNTER = "counter"
const TABLE_NAME_ACCOUNTS = "accounts"
const TABLE_NAME_USERS = "users"

const KEY_USER_ID = "uid"
