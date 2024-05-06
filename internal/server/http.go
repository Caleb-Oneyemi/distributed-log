package server

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Caleb-Oneyemi/distributed-log/internal/memory"
	"github.com/gorilla/mux"
)

type httpServer struct {
	Log *memory.Log
}

func newHTTPServer() *httpServer {
	return &httpServer{
		Log: memory.NewLog(),
	}
}

func NewHttpServer(addr string) *http.Server {
	server := newHTTPServer()
	router := mux.NewRouter()

	router.HandleFunc("/", server.Produce).Methods("POST")
	router.HandleFunc("/", server.Consume).Methods("GET")

	return &http.Server{
		Addr:    addr,
		Handler: router,
	}
}

type ProducerRequest struct {
	Record memory.Record `json:"record"`
}

type ProducerResponse struct {
	Offset uint64 `json:"offset"`
}

type ConsumerRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumerResponse struct {
	Record memory.Record `json:"record"`
}

func (s *httpServer) Produce(w http.ResponseWriter, r *http.Request) {
	var input ProducerRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	offset, err := s.Log.Append(input.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ProducerResponse{Offset: offset}
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *httpServer) Consume(w http.ResponseWriter, r *http.Request) {
	var input ConsumerRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	record, err := s.Log.Read(input.Offset)
	if err != nil && errors.Is(err, memory.ErrOffsetNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := ConsumerResponse{Record: record}
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
