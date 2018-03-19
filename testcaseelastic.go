package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"reflect"
	"strconv"

	elastic "github.com/olivere/elastic"
)

const (
	MainIndex    string = "marketplace"
	ContentIndex string = "placeproduct"
	MapDefault   string = `{
		"settings": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		}
	}`
)

const MappingMain = `
	{"mappings":{
		"category": {
			"properties": {
		  		"id":{"type":"long"},
		  		"code": {"type": "string"},
		  		"name": {"type": "string"},
		  		"description":{"type": "string"},
		  		"parentid":{"type":"long"}			 
			}
		}
	}
	`
const MappingProduct = `
	{"mappings":{
		"product": {
			"properties": {
		   		"id":{"type":"long"},
		   		"code": {"type": "string"},
		   		"name": {"type": "string"},
		   		"description":{"type": "string"},
		   		"category":{"type":"string"},
		   		"harga":{"type":"long"}
	 		}
		}
	}
}
`

type category struct {
	Id          int64  `json:id`
	Code        string `json:code`
	Name        string `json:name`
	Description string `json:description`
	Parentid    int64  `json:parentid`
}

type product struct {
	Id          int64  `json:id`
	Code        string `json:code`
	Name        string `json:name`
	Description string `json:description`
	Category    string `json:category`
	Harga       int64  `json:harga`
}

type varprior struct {
	Index    string `json:index`
	PriorVar string `json:priorvar`
}

func bulkIndexCat(client *elastic.Client, index string, typ string, data []category) {
	ctx := context.Background()
	for idx, item := range data {
		idxs := strconv.Itoa(idx)
		_, err := client.Index().Index(index).Type(typ).Id(idxs).BodyJson(item).Do(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}
}
func bulkIndexPro(client *elastic.Client, index string, typ string, data []product) {
	ctx := context.Background()
	for idx, item := range data {
		idxs := strconv.Itoa(idx)
		_, err := client.Index().Index(index).Type(typ).Id(idxs).BodyJson(item).Do(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}
}

/*
func showPos(client *elastic.Client, index []string, valueprior []varprior, value string) {
	ctx := context.Background()
	var sdatacategory category
	var sdataproduct product
	for _, tmpIndex := range index {
		for _, tmpvar := range valueprior {
			log.Println("Searching in:" + tmpIndex + " in var: " + tmpvar.PriorVar)
			termQuery := elastic.NewTermQuery(tmpvar.PriorVar, value)
			temp, err := client.Search().Index(tmpIndex).Query(termQuery).Do(ctx)
			if err != nil {
				panic(err)
			}
			if tmpIndex == "placeproduct" {
				for _, itemP := range temp.Each(reflect.TypeOf(sdataproduct)) {
					t := itemP.(product)
					log.Println("Code : " + t.Code + "|| Name : " + t.Name)
				}
			} else {
				for _, itemC := range temp.Each(reflect.TypeOf(sdatacategory)) {
					t := itemC.(category)
					log.Println("Code : " + t.Code + "|| Name : " + t.Name)
				}
			}
		}
	}
}*/

func CreateCategory(client *elastic.Client, indexName string, indexType string, newCategory category) {
	ctx := context.Background()
	//get last id in index
	sum, err := client.Count(MainIndex).Do(ctx)
	if err != nil {
		log.Fatal(err)
	}
	sumi := strconv.FormatInt((sum), 16)

	newCategory.Id = sum
	client.Index().Index(indexName).Type(indexType).Id(sumi).BodyJson(newCategory).Do(ctx)
	//put data di last id
	log.Println("new Category has been created...")

}

func ReadCategory(client *elastic.Client, indexName string) {
	//elastic Search
	ctx := context.Background()
	resultc, err := client.Search().Index(MainIndex).Do(ctx)
	if err != nil {
		log.Println("ini salah?")
		panic(err)
	}

	var sdatacategory category
	for _, itemC := range resultc.Each(reflect.TypeOf(sdatacategory)) {
		t := itemC.(category)
		log.Println("Code : " + t.Code + "|| Name : " + t.Name)
	}

}

func UpdateCategory(client *elastic.Client, indexName string, typeIndex string, value category, updateValue category) {
	//get category id by search
	log.Printf("data with ID:%d will be updated \n", updateValue.Id)
	client.Index().Index(indexName).Type(typeIndex).Id(strconv.FormatInt(updateValue.Id, 10)).BodyJson(updateValue).Do(context.Background())
	log.Println("Data has been Updated")
	//update process
}

func DeleteCategory(client *elastic.Client, indexName string, indexType string, value category) {
	//get category id by search
	//delete process
	log.Println("Deleting data with ID:", value.Id)
	_, err := client.Delete().Index(indexName).Type(indexType).Id(strconv.FormatInt(value.Id, 10)).Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}

}

func DeleteIndex(client *elastic.Client, indexName string) {
	ctx := context.Background()
	_, err := client.DeleteIndex(indexName).Do(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

/*
func searchCat(client *elastic.Client, typeName string, value category) (id int) {
	//parse value into
	term := `{
		"query":{
			"term":{
				"Code":` + value.Code + `,
				"Id":` + strconv.FormatInt(value.Id, 10) + `,
				"Name":` + value.Name + `,
				"ParentId":` + strconv.FormatInt(value.Parentid, 10) + `,
				"Description":` + value.Description + `
			}
		}
	}`

	r, err := client.Search().Index(MainIndex).Do(context.Background())
	if err != nil {
		log.Println(err)
	}
	if r != nil {
		log.Println(r)
		return 0
	}
	return 0
}
*/
func main() {
	//preparation
	//ListIndex := []string{ContentIndex, MainIndex}

	ctx := context.Background()
	client, err := elastic.NewClient()
	if err != nil {
		// Handle error
		log.Printf("Nothing Server Available\n")
		panic(err)
	}
	log.Printf("Connected to 127.0.0.1:9200\n")

	exists1, err := client.IndexExists(MainIndex).Do(ctx)
	exists2, err2 := client.IndexExists(ContentIndex).Do(ctx)
	if err != nil || err2 != nil {
		// Handle error
		log.Println(err)
		log.Println("index not Found, Prepare to make new index")
	}
	if !exists1 {
		log.Println("jalan")
		// Index does not exist yet.
		//make := client.CreateIndex(MainIndex).Body(`PUT pokedex{"settings":{"number_of_shards":"index":{1,"number_of_replicas": 0}}}`)
		make, err := client.CreateIndex(MainIndex).BodyString(MapDefault).Do(ctx)
		if err != nil {
			log.Println("salah disini")
			log.Println(err)
			log.Fatal("udahan")
		} else {
			log.Println("Index Created with name: " + MainIndex)
		}
		log.Println(make)

		//mapping index first
		mape, err := client.PutMapping().Index(MainIndex).Type("category").BodyString(MappingMain).Do(ctx)
		if err != nil {
			log.Println(err)
			log.Fatal("udahan aja")
		} else {
			log.Println("Type Succesfull created ... ")
		}
		log.Println(mape)

		//input Data from JSON file if index nil
		//preparation
		var datacategory []category
		//check index != nil
		//inputprocess

		fileCat, err1 := os.Open("categoryindex.json")
		if err1 != nil {
			log.Fatal(err)
		}

		jsonDecoderCat := json.NewDecoder(fileCat)
		if err := jsonDecoderCat.Decode(&datacategory); err != nil {
			log.Fatal("Decode:", err)
		}
		log.Println("Parsing data from Json to TempData")

		bulkIndexCat(client, MainIndex, "category", datacategory)
	}

	if !exists2 {
		log.Println("jalan")
		// Index does not exist yet.
		//make := client.CreateIndex(MainIndex).Body(`PUT pokedex{"settings":{"number_of_shards":"index":{1,"number_of_replicas": 0}}}`)
		make, err := client.CreateIndex(ContentIndex).BodyString(MapDefault).Do(ctx)
		if err != nil {
			log.Println(err)
			log.Fatal("udahan")
		} else {
			log.Println("Index Created with name: " + ContentIndex)
		}
		log.Println(make)

		//mapping index first
		mape, err := client.PutMapping().Index(ContentIndex).Type("product").BodyString(MappingProduct).Do(ctx)
		if err != nil {
			log.Println(err)
			log.Fatal("udahan aja")
		} else {
			log.Println("Type Succesfull created ... ")
		}
		log.Println(mape)

		//input Data from JSON file if index nil
		//preparation
		var dataproduct []product
		filePro, err := os.Open("productindex.json")
		if err != nil {
			log.Fatal(err)
		}
		jsonDecoderPro := json.NewDecoder(filePro)
		if err := jsonDecoderPro.Decode(&dataproduct); err != nil {
			log.Fatal("Decode:", err)
		}
		log.Println("Parsing data from Json to TempData")
		bulkIndexPro(client, ContentIndex, "product", dataproduct)
	}

	/*//elastic Search
	// Set up the request JSON manually to pass to the search service via Source().Query(elastic.NewMatchAllQuery()).Pretty(true)
	resultc, err := client.Search().Index(MainIndex).Do(ctx)
	resultp, err := client.Search().Index(ContentIndex).Do(ctx)
	if err != nil {
		log.Println("ini salah?")
		panic(err)
	}

		var sdatacategory category
		var sdataproduct product
		for _, itemC := range resultc.Each(reflect.TypeOf(sdatacategory)) {
			t := itemC.(category)
			log.Println("Code : " + t.Code + "|| Name : " + t.Name)
		}

		for _, itemP := range resultp.Each(reflect.TypeOf(sdataproduct)) {
			p := itemP.(product)
			log.Println("Code : " + p.Code + "|| Name : " + p.Name)
		}
	*/
	//multisearch with priority.
	prior := []varprior{{ContentIndex, "Name"}, {ContentIndex, "Description"}, {ContentIndex, "Code"}, {MainIndex, "Name"}, {MainIndex, "Description"}, {MainIndex, "Code"}}

	showPos(client, ListIndex, prior, "pakaian")
	

	/*
		//search by query
		termQuery := elastic.NewTermQuery("Name", "jeans")
		var termString string = `{
			"query": {
			  "term" : { "Name" : "Jeans" }
			}
		}`
		temp, err := client.Search().
			Index(ContentIndex).
			Query(termQuery).
			Do(ctx)
		if err != nil {
			panic(err)
		}

		log.Printf("Search complete with time: %d\n", temp.TookInMillis)

		if temp.Hits.TotalHits > 0 {
			log.Printf("Total data has match with query %d \n", temp.Hits.TotalHits)

			//loop out search result
			for _, hit := range temp.Hits.Hits {
				var p product
				err := json.Unmarshal(*hit.Source, &p)
				if err != nil {
					log.Println("Fail Macthing...")
				}
				log.Printf("Barang: %s || %s", p.Code, p.Name)
			}
		}
	*/
	/*
		newCat := category{
			Id:          8,
			Code:        "C0x1",
			Name:        "Kategoribaru",
			Description: "Nope",
			Parentid:    0,
		}*/
	//CreateCategory(client, MainIndex, "category", newCat)

	value := category{
		Id: 8,
	}
	//UpdateCategory(client, MainIndex, "category", value, newCat)
	DeleteCategory(client, MainIndex, "category", value)

}
