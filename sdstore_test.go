package sdstore_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/toqns/sdstore"
)

// Success and failed markers.
const (
	success = "\u2713"
	failed  = "\u2717"
)

type Record struct {
	ID    string
	Name  string
	Email string
}

func TestStore(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("/tmp/test") })

	store, err := sdstore.New("store", "/tmp/test")
	if err != nil {
		t.Fatalf("%s\tShould be able to create a new store: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create a new store.", success)

	c, err := store.Collection("test", Record{})
	if err != nil {
		t.Fatalf("%s\tShould be able to create a new collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create a new collection.", success)

	o := Record{
		ID:    "1",
		Name:  "Test",
		Email: "test@example.com",
	}

	if err := c.Create(o.ID, o); err != nil {
		t.Fatalf("%s\tShould be able to create an object in the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create an object in the collection.", success)

	got := Record{}
	if err := c.Get("1", &got); err != nil {
		t.Fatalf("%s\tShould be able to get an object by ID from the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to get an object by ID from the collection.", success)

	if diff := cmp.Diff(got, o); diff != "" {
		t.Fatalf("%s\tShould get expected result: %v.", failed, diff)
	}
	t.Logf("%s\tShould get expected result.", success)

	res, err := c.Query(func(r any) bool {
		rec, ok := r.(*Record)
		if !ok {
			return false
		}
		fmt.Println(rec)

		return rec.ID == "1"
	})
	if err != nil {
		t.Fatalf("%s\tShould be able to query from the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to query from the collection.", success)

	if got, exp := len(res), 1; got != exp {
		t.Fatalf("%s\tShould get expected result %d: %d.", failed, exp, got)
	}
	t.Logf("%s\tShould get expected result.", success)

	o.Name = "John Doe"
	if err := c.Update(o.ID, o); err != nil {
		t.Fatalf("%s\tShould be able to update an object in the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to update an object in the collection.", success)

	upd := Record{}
	if err := c.Get("1", &upd); err != nil {
		t.Fatalf("%s\tShould be able to get updated object from the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to get updated from the collection.", success)

	if diff := cmp.Diff(upd, o); diff != "" {
		t.Fatalf("%s\tShould get expected result: %v.", failed, diff)
	}
	t.Logf("%s\tShould get expected result.", success)

	if err := c.Delete("1"); err != nil {
		t.Fatalf("%s\tShould be able to delete an object by ID from the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to delete an object by ID from the collection.", success)

	tmp := Record{}
	if err := c.Get("1", &tmp); err != sdstore.ErrNotFound {
		t.Fatalf("%s\tShould get ErrNotFound when getting an deleted object: %v.", failed, err)
	}
	t.Logf("%s\tShould get ErrNotFound when getting an deleted object.", success)
}

func TestIndexedCollection(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("/tmp/test") })

	store, err := sdstore.New("store", "/tmp/test")
	if err != nil {
		t.Fatalf("%s\tShould be able to create a new store: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create a new store.", success)

	c, err := store.Collection("test", Record{}, sdstore.WithIndexedFields("Email"))
	if err != nil {
		t.Fatalf("%s\tShould be able to create a new collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create a new collection.", success)

	o := Record{
		ID:    "1",
		Name:  "Test",
		Email: "test@example.com",
	}

	if err := c.Create(o.ID, o); err != nil {
		t.Fatalf("%s\tShould be able to create an object in the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create an object in the collection.", success)

	got := Record{}
	if err := c.Get("1", &got); err != nil {
		t.Fatalf("%s\tShould be able to get an object by ID from the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to get an object by ID from the collection.", success)

	if diff := cmp.Diff(got, o); diff != "" {
		t.Fatalf("%s\tShould get expected result: %v.", failed, diff)
	}
	t.Logf("%s\tShould get expected result.", success)

	gotIndexed := Record{}
	if err := c.GetIndexed("Email", "test@example.com", &gotIndexed); err != nil {
		t.Fatalf("%s\tShould be able to get an object by Email from the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to get an object by Email from the collection.", success)

	if diff := cmp.Diff(gotIndexed, o); diff != "" {
		t.Fatalf("%s\tShould get expected result: %v.", failed, diff)
	}
	t.Logf("%s\tShould get expected result.", success)

	if err := c.GetIndexed("Name", "Test", &gotIndexed); err != sdstore.ErrNotFound {
		t.Fatalf("%s\tShould get ErrNotFound when getting non-indexed field: %v.", failed, err)
	}
	t.Logf("%s\tShould get ErrNotFound when getting non-indexed field.", success)

	res, err := c.Query(func(r any) bool {
		rec, ok := r.(*Record)
		if !ok {
			return false
		}

		return rec.ID == "1"
	})
	if err != nil {
		t.Fatalf("%s\tShould be able to query from the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to query from the collection.", success)

	if got, exp := len(res), 1; got != exp {
		t.Fatalf("%s\tShould get expected result %d: %d.", failed, exp, got)
	}
	t.Logf("%s\tShould get expected result.", success)

	o.Email = "john.doe@example.com"
	if err := c.Update(o.ID, o); err != nil {
		t.Fatalf("%s\tShould be able to update an object in the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to update an object in the collection.", success)

	upd := Record{}
	if err := c.GetIndexed("Email", "john.doe@example.com", &upd); err != nil {
		t.Fatalf("%s\tShould be able to get updated indexed object from the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to get updated indexed object from the collection.", success)

	if diff := cmp.Diff(upd, o); diff != "" {
		t.Fatalf("%s\tShould get expected result: %v.", failed, diff)
	}
	t.Logf("%s\tShould get expected result.", success)

	c2, err := store.Collection("test", Record{}, sdstore.WithIndexedFields("Email"))
	if err != nil {
		t.Fatalf("%s\tShould be able to create another collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create another collection.", success)

	c2r := Record{}
	if err := c2.GetIndexed("Email", "john.doe@example.com", &c2r); err != nil {
		t.Fatalf("%s\tShould be able to get indexed object from the other collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to get indexed object from the other collection.", success)

	if err := c.Delete("1"); err != nil {
		t.Fatalf("%s\tShould be able to delete an object by ID from the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to delete an object by ID from the collection.", success)

	tmp := Record{}
	if err := c.Get("1", &tmp); err != sdstore.ErrNotFound {
		t.Fatalf("%s\tShould get ErrNotFound when getting an deleted object: %v.", failed, err)
	}
	t.Logf("%s\tShould get ErrNotFound when getting an deleted object.", success)
}

func TestPagination(t *testing.T) {
	t.Cleanup(func() { os.RemoveAll("/tmp/pgstore") })

	store, err := sdstore.New("store", "/tmp/pgstore")
	if err != nil {
		t.Fatalf("%s\tShould be able to create a new store: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create a new store.", success)

	c, err := store.Collection("test", Record{})
	if err != nil {
		t.Fatalf("%s\tShould be able to create a new collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create a new collection.", success)

	rec1 := Record{
		ID:    "1",
		Name:  "Test",
		Email: "test1@example.com",
	}

	rec2 := Record{
		ID:    "2",
		Name:  "Test",
		Email: "test2@example.com",
	}

	if err := c.Create(rec1.ID, rec1); err != nil {
		t.Fatalf("%s\tShould be able to create an object in the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create an object in the collection.", success)

	if err := c.Create(rec2.ID, rec2); err != nil {
		t.Fatalf("%s\tShould be able to create another object in the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to create another object in the collection.", success)

	recs, err := c.QueryPaginated(func(rec any) bool {
		return true
	}, 1, 1)
	if err != nil {
		t.Fatalf("%s\tShould be able to query from the collection: %v.", failed, err)
	}
	t.Logf("%s\tShould be able to query from the collection.", success)

	if l := len(recs); l != 1 {
		t.Fatalf("%s\tShould get 1 record, but got: %v.", failed, l)
	}
	t.Logf("%s\tShould get 1 record.", success)

	got := *(recs[0].(*Record))
	exp := rec1
	if diff := cmp.Diff(got, exp); diff != "" {
		t.Fatalf("%s\tShould get expected result: %v.", failed, diff)
	}
	t.Logf("%s\tShould get expected result.", success)
}