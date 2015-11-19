package dataio
import "time"

const URL_dynamoDB = "http://localhost:8000"
const URL_REDIS = "127.0.0.1:6379"
const NUM_REDIS_DB	= 6

const TTL_CACHE_USER_DATA = 24*60*60*2
const TIMEOUT = 3 * time.Minute

const DEBUG_MODE_LOG = false
const DEBUG_MODE_UNIT_CONSUMED_LOG = false
//const URL_dynamoDB = "us-west-2"


const TABLE_NAME_COUNTER = "counter"
const TABLE_NAME_ACCOUNTS = "accounts"

const KEY_USER_ID = "uid"

const NULL_NUMBER = -99990000

const KEY_DB_USER = "users"
const KEY_DB_TASK = "usertasks"

const KEY_CACHE_USER = "u"
const KEY_CACHE_TASK = "t"

const KEY_USER = "user"
const KEY_TASK = "task"
