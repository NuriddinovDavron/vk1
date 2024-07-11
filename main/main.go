package main

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/Shopify/sarama"
)

type TDocument struct {
	URL            string `json:"url"`
	PubDate        uint64 `json:"pub_date"`
	FetchTime      uint64 `json:"fetch_time"`
	Text           string `json:"text"`
	FirstFetchTime uint64 `json:"first_fetch_time"`
}

type DocumentDB interface {
	GetDocumentsByURL(url string) ([]*TDocument, error)
	SaveDocument(d *TDocument) error
}

type InMemoryDocumentDB struct {
	documents map[string][]*TDocument
}

func NewInMemoryDocumentDB() *InMemoryDocumentDB {
	return &InMemoryDocumentDB{
		documents: make(map[string][]*TDocument),
	}
}

func main() {
	db := NewInMemoryDocumentDB()
	processor := &DocumentProcessor{db: db}

	// Set up Kafka consumer
	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalf("Failed to start Kafka consumer: %s", err)
	}
	defer consumer.Close()

	// Set up Kafka producer
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalf("Failed to start Kafka producer: %s", err)
	}
	defer producer.Close()

	partitionConsumer, err := consumer.ConsumePartition("input_topic", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start partition consumer: %s", err)
	}
	defer partitionConsumer.Close()

	for message := range partitionConsumer.Messages() {
		var doc TDocument
		err := json.Unmarshal(message.Value, &doc)
		if err != nil {
			log.Printf("Failed to unmarshal message: %s", err)
			continue
		}

		processedDoc, err := processor.Process(&doc)
		if err != nil {
			log.Printf("Failed to process document: %s", err)
			continue
		}

		if processedDoc != nil {
			outputMessage, err := json.Marshal(processedDoc)
			if err != nil {
				log.Printf("Failed to marshal output message: %s", err)
				continue
			}

			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Topic: "output_topic",
				Value: sarama.ByteEncoder(outputMessage),
			})
			if err != nil {
				log.Printf("Failed to send output message: %s", err)
			}
		}
	}
}

func (db *InMemoryDocumentDB) GetDocumentsByURL(url string) ([]*TDocument, error) {
	if docs, ok := db.documents[url]; ok {
		return docs, nil
	}
	return []*TDocument{}, nil
}

func (db *InMemoryDocumentDB) SaveDocument(d *TDocument) error {
	db.documents[d.URL] = append(db.documents[d.URL], d)
	return nil
}

type Processor interface {
	Process(d *TDocument) (*TDocument, error)
}

type DocumentProcessor struct {
	db DocumentDB
}

func (p *DocumentProcessor) Process(d *TDocument) (*TDocument, error) {
	if d == nil {
		return nil, nil
	}

	// Get the current document versions by URL
	docs, err := p.db.GetDocumentsByURL(d.URL)
	if err != nil {
		return nil, err
	}

	// Update the document versions in the database
	err = p.db.SaveDocument(d)
	if err != nil {
		return nil, err
	}

	// Determine the new fields based on the provided rules
	var minFetchTime, maxFetchTime uint64
	var minPubDate uint64
	var latestText string

	if len(docs) == 0 {
		minFetchTime = d.FetchTime
		maxFetchTime = d.FetchTime
		minPubDate = d.PubDate
		latestText = d.Text
	} else {
		minFetchTime = docs[0].FetchTime
		maxFetchTime = docs[0].FetchTime
		minPubDate = docs[0].PubDate
		latestText = docs[0].Text

		for _, doc := range docs {
			if doc.FetchTime < minFetchTime {
				minFetchTime = doc.FetchTime
				minPubDate = doc.PubDate
			}
			if doc.FetchTime > maxFetchTime {
				maxFetchTime = doc.FetchTime
				latestText = doc.Text
			}
		}

		if d.FetchTime < minFetchTime {
			minFetchTime = d.FetchTime
			minPubDate = d.PubDate
		}
		if d.FetchTime > maxFetchTime {
			maxFetchTime = d.FetchTime
			latestText = d.Text
		}
	}

	outputDoc := &TDocument{
		URL:            d.URL,
		PubDate:        minPubDate,
		FetchTime:      d.FetchTime,
		Text:           latestText,
		FirstFetchTime: minFetchTime,
	}

	return outputDoc, nil
}

func TestDocumentProcessor(t *testing.T) {
	db := NewInMemoryDocumentDB()
	processor := &DocumentProcessor{db: db}

	doc1 := &TDocument{
		URL:       "doc1",
		PubDate:   1620000000,
		FetchTime: 1620001000,
		Text:      "First version of document",
	}

	processedDoc, err := processor.Process(doc1)
	if err != nil {
		t.Fatalf("Error processing document: %s", err)
	}

	if processedDoc == nil {
		t.Fatalf("Expected a processed document, got nil")
	}

	if processedDoc.FirstFetchTime != doc1.FetchTime {
		t.Errorf("Expected FirstFetchTime to be %d, got %d", doc1.FetchTime, processedDoc.FirstFetchTime)
	}

	if processedDoc.Text != doc1.Text {
		t.Errorf("Expected Text to be '%s', got '%s'", doc1.Text, processedDoc.Text)
	}
}
