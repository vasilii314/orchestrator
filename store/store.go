package store

type StoreType string

const (
	InMemoryStore StoreType = "memory"
)

type Store[K comparable, V any] interface {
	Put(key K, value V) error
	Get(key K) (V, error)
	List() ([]V, error)
	Count() (int, error)
}
