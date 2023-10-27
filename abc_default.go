package abc

var (
	dflt ABC
)

func Init(key, secret, endpoint, region string) {
	dflt.Init(key, secret, endpoint, region)
}

func Get(bucket string, key string, x interface{}) error {
	return dflt.Get(bucket, key, x)
}

func Put(bucket string, key string, x interface{}) error {
	return dflt.Put(bucket, key, x)
}
