package db

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

func TestRelaxJSON(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"bare key", `{ username : "sofroesch" }`, `{ "username" : "sofroesch" }`},
		{"single quotes", `{name: 'lucas'}`, `{"name": "lucas"}`},
		{"trailing comma object", `{a: 1,}`, `{"a": 1}`},
		{"ObjectId literal", `{_id: ObjectId("507f1f77bcf86cd799439011")}`, `{"_id": {"$oid":"507f1f77bcf86cd799439011"}}`},
		{"ISODate literal", `{ts: ISODate("2025-01-01T00:00:00Z")}`, `{"ts": {"$date":"2025-01-01T00:00:00Z"}}`},
		{"quoted keys untouched", `{"x": 1}`, `{"x": 1}`},
		{"dollar operator", `{$set: {a: 1}}`, `{"$set": {"a": 1}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := relaxJSON(c.in)
			if got != c.want {
				t.Errorf("relaxJSON(%q)\n  got  %q\n  want %q", c.in, got, c.want)
			}
			var out bson.M
			if err := unmarshalMongoJSON(c.in, &out); err != nil {
				t.Errorf("input failed to parse via unmarshalMongoJSON: %v", err)
			}
		})
	}
}

func TestParseShellQueryProjection(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{
			"findOne with projection",
			`db.users.findOne({username:"x"}, {username:1, role:1})`,
			`find users {username:"x"} 1 projection:{username:1, role:1}`,
		},
		{
			"find with projection",
			`db.users.find({active:true}, {email:1})`,
			`find users {active:true} projection:{email:1}`,
		},
		{
			"findOne without projection",
			`db.users.findOne({username:"x"})`,
			`find users {username:"x"} 1`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := parseShellQuery(c.in)
			if !ok {
				t.Fatalf("parseShellQuery returned !ok for %q", c.in)
			}
			if got != c.want {
				t.Errorf("parseShellQuery(%q)\n  got  %q\n  want %q", c.in, got, c.want)
			}
		})
	}
}
