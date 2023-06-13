package model

type Order struct {
	Id       uint64  `json:"id"`
	ClientId uint64  `json:"client"`
	Email    string  `json:"e-mail"`
	Price    float64 `json:"price"`
}
