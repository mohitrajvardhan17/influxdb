package testing

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

const (
	bucketOneID   = "020f755c3c082000"
	bucketTwoID   = "020f755c3c082001"
	bucketThreeID = "020f755c3c082002"
)

// taskBucket := influxdb.Bucket{
// 	ID:              influxdb.TasksSystemBucketID,
// 	Name:            "_tasks",
// 	RetentionPeriod: time.Hour * 24 * 3,
// 	Description:     "System bucket for task logs",
// }
//
// monitoringBucket := influxdb.Bucket{
// 	ID:              influxdb.MonitoringSystemBucketID,
// 	Name:            "_monitoring",
// 	RetentionPeriod: time.Hour * 24 * 7,
// 	Description:     "System bucket for monitoring logs",
// }

var bucketCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Bucket) []*influxdb.Bucket {
		out := append([]*influxdb.Bucket(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// BucketFields will include the IDGenerator, and buckets
type BucketFields struct {
	IDGenerator   influxdb.IDGenerator
	OrgBucketIDs  influxdb.IDGenerator
	TimeGenerator influxdb.TimeGenerator
	Buckets       []*influxdb.Bucket
	Organizations []*influxdb.Organization
}

type bucketServiceF func(
	init func(BucketFields, *testing.T) (influxdb.BucketService, string, func()),
	t *testing.T,
)

// BucketService tests all the service functions.
func BucketService(
	init func(BucketFields, *testing.T) (influxdb.BucketService, string, func()),
	t *testing.T,
) {
	tests := []struct {
		name string
		fn   bucketServiceF
	}{
		{
			name: "CreateBucket",
			fn:   CreateBucket,
		},
		{
			name: "FindBucketByID",
			fn:   FindBucketByID,
		},
		{
			name: "FindBuckets",
			fn:   FindBuckets,
		},
		{
			name: "FindBucket",
			fn:   FindBucket,
		},
		{
			name: "UpdateBucket",
			fn:   UpdateBucket,
		},
		{
			name: "DeleteBucket",
			fn:   DeleteBucket,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateBucket testing
func CreateBucket(
	init func(BucketFields, *testing.T) (influxdb.BucketService, string, func()),
	t *testing.T,
) {
	type args struct {
		bucket *influxdb.Bucket
	}
	type wants struct {
		err     error
		buckets []*influxdb.Bucket
	}

	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "create buckets with empty set",
			fields: BucketFields{
				IDGenerator:   mock.NewIDGenerator(bucketOneID, t),
				OrgBucketIDs:  mock.NewIDGenerator(bucketOneID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Buckets:       []*influxdb.Bucket{},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				bucket: &influxdb.Bucket{
					Name:        "name1",
					OrgID:       MustIDBase16(orgOneID),
					Description: "desc1",
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						Name:        "name1",
						ID:          MustIDBase16(bucketOneID),
						OrgID:       MustIDBase16(orgOneID),
						Description: "desc1",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
				},
			},
		},
		{
			name: "basic create bucket",
			fields: BucketFields{
				IDGenerator:   mock.NewIDGenerator(bucketTwoID, t),
				OrgBucketIDs:  mock.NewIDGenerator(bucketTwoID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
			},
			args: args{
				bucket: &influxdb.Bucket{
					Name:  "bucket2",
					OrgID: MustIDBase16(orgTwoID),
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						Name:  "bucket2",
						OrgID: MustIDBase16(orgTwoID),
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
				},
			},
		},
		{
			name: "names should be unique within an organization",
			fields: BucketFields{
				IDGenerator:   mock.NewIDGenerator(bucketTwoID, t),
				OrgBucketIDs:  mock.NewIDGenerator(bucketTwoID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
			},
			args: args{
				bucket: &influxdb.Bucket{
					Name:  "bucket1",
					OrgID: MustIDBase16(orgOneID),
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
				},
				err: &influxdb.Error{
					Code: influxdb.EConflict,
					Op:   influxdb.OpCreateBucket,
					Msg:  fmt.Sprintf("bucket with name bucket1 already exists"),
				},
			},
		},
		{
			name: "ids should be unique",
			fields: BucketFields{
				IDGenerator:   mock.NewIDGenerator(bucketOneID, t),
				OrgBucketIDs:  mock.NewIDGenerator(bucketOneID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				bucket: &influxdb.Bucket{
					Name:  "bucket2",
					OrgID: MustIDBase16(orgOneID),
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
				},
				err: &influxdb.Error{
					Code: influxdb.EInternal,
					Msg:  fmt.Sprintf("unable to generate valid id"),
				},
			},
		},
		{
			name: "reserved ids should not be created",
			fields: BucketFields{
				IDGenerator:   mock.NewIDGenerator("000000000000000a", t),
				OrgBucketIDs:  mock.NewIDGenerator("000000000000000a", t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				bucket: &influxdb.Bucket{
					Name:  "bucket2",
					OrgID: MustIDBase16(orgOneID),
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
				},
				err: &influxdb.Error{
					Code: influxdb.EInternal,
					Msg:  fmt.Sprintf("unable to generate valid id"),
				},
			},
		},
		{
			name: "names should not be unique across organizations",
			fields: BucketFields{
				IDGenerator:   mock.NewIDGenerator(bucketTwoID, t),
				OrgBucketIDs:  mock.NewIDGenerator(bucketTwoID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				bucket: &influxdb.Bucket{
					Name:  "bucket1",
					OrgID: MustIDBase16(orgTwoID),
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgOneID),
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						Name:  "bucket1",
						OrgID: MustIDBase16(orgTwoID),
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
				},
			},
		},
		{
			name: "create bucket with orgID not exist",
			fields: BucketFields{
				IDGenerator:   mock.NewIDGenerator(bucketOneID, t),
				OrgBucketIDs:  mock.NewIDGenerator(bucketOneID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Buckets:       []*influxdb.Bucket{},
				Organizations: []*influxdb.Organization{},
			},
			args: args{
				bucket: &influxdb.Bucket{
					Name:  "name1",
					OrgID: MustIDBase16(orgOneID),
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{},
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "organization not found",
					Op:   influxdb.OpCreateBucket,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.CreateBucket(ctx, tt.args.bucket)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			// Delete only newly created buckets - ie., with a not nil ID
			// if tt.args.bucket.ID.Valid() {
			defer s.DeleteBucket(ctx, tt.args.bucket.ID)
			// }

			buckets, _, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
			if err != nil {
				t.Fatalf("failed to retrieve buckets: %v", err)
			}

			// remove system buckets
			filteredBuckets := []*influxdb.Bucket{}
			for _, b := range buckets {
				if b.ID > 15 {
					filteredBuckets = append(filteredBuckets, b)
				}
			}

			if diff := cmp.Diff(filteredBuckets, tt.wants.buckets, bucketCmpOptions...); diff != "" {
				t.Errorf("buckets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindBucketByID testing
func FindBucketByID(
	init func(BucketFields, *testing.T) (influxdb.BucketService, string, func()),
	t *testing.T,
) {
	type args struct {
		id influxdb.ID
	}
	type wants struct {
		err    error
		bucket *influxdb.Bucket
	}

	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "basic find bucket by id",
			fields: BucketFields{
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket1",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket2",
					},
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(bucketTwoID),
			},
			wants: wants{
				bucket: &influxdb.Bucket{
					ID:    MustIDBase16(bucketTwoID),
					OrgID: MustIDBase16(orgOneID),
					Name:  "bucket2",
				},
			},
		},
		{
			name: "find bucket by id not exist",
			fields: BucketFields{
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket1",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket2",
					},
				},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindBucketByID,
					Msg:  "bucket not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			bucket, err := s.FindBucketByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(bucket, tt.wants.bucket, bucketCmpOptions...); diff != "" {
				t.Errorf("bucket is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindBuckets testing
func FindBuckets(
	init func(BucketFields, *testing.T) (influxdb.BucketService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID             influxdb.ID
		name           string
		organization   string
		organizationID influxdb.ID
		findOptions    influxdb.FindOptions
	}

	type wants struct {
		buckets []*influxdb.Bucket
		err     error
	}
	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "find all buckets",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgTwoID),
						Name:  "xyz",
					},
				},
			},
			args: args{},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgTwoID),
						Name:  "xyz",
					},
				},
			},
		},
		{
			name: "find all buckets by offset and limit",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "def",
					},
					{
						ID:    MustIDBase16(bucketThreeID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "xyz",
					},
				},
			},
			args: args{
				findOptions: influxdb.FindOptions{
					Offset: 1,
					Limit:  1,
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "def",
					},
				},
			},
		},
		{
			name: "find all buckets by descending",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "def",
					},
					{
						ID:    MustIDBase16(bucketThreeID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "xyz",
					},
				},
			},
			args: args{
				findOptions: influxdb.FindOptions{
					Offset:     1,
					Descending: true,
				},
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "def",
					},
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
				},
			},
		},
		{
			name: "find buckets by organization name",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgTwoID),
						Name:  "xyz",
					},
					{
						ID:    MustIDBase16(bucketThreeID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "123",
					},
				},
			},
			args: args{
				organization: "theorg",
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
					{
						ID:    MustIDBase16(bucketThreeID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "123",
					},
				},
			},
		},
		{
			name: "find buckets by organization id",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "otherorg",
						ID:   MustIDBase16(orgTwoID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgTwoID),
						Name:  "xyz",
					},
					{
						ID:    MustIDBase16(bucketThreeID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "123",
					},
				},
			},
			args: args{
				organizationID: MustIDBase16(orgOneID),
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
					{
						ID:    MustIDBase16(bucketThreeID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "123",
					},
				},
			},
		},
		{
			name: "find bucket by name",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "xyz",
					},
				},
			},
			args: args{
				name: "xyz",
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "xyz",
					},
				},
			},
		},
		{
			name: "missing bucket returns no buckets",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{},
			},
			args: args{
				name: "xyz",
			},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			filter := influxdb.BucketFilter{}
			if tt.args.ID.Valid() {
				filter.ID = &tt.args.ID
			}
			if tt.args.organizationID.Valid() {
				filter.OrganizationID = &tt.args.organizationID
			}
			if tt.args.organization != "" {
				filter.Org = &tt.args.organization
			}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}

			buckets, _, err := s.FindBuckets(ctx, filter, tt.args.findOptions)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			// remove system buckets
			filteredBuckets := []*influxdb.Bucket{}
			for _, b := range buckets {
				if b.ID > 15 {
					filteredBuckets = append(filteredBuckets, b)
				}
			}

			if diff := cmp.Diff(filteredBuckets, tt.wants.buckets, bucketCmpOptions...); diff != "" {
				t.Errorf("buckets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteBucket testing
func DeleteBucket(
	init func(BucketFields, *testing.T) (influxdb.BucketService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID string
	}
	type wants struct {
		err     error
		buckets []*influxdb.Bucket
	}

	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "delete buckets using exist id",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						Name:  "A",
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
					},
					{
						Name:  "B",
						ID:    MustIDBase16(bucketThreeID),
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				ID: bucketOneID,
			},
			wants: wants{
				buckets: []*influxdb.Bucket{
					{
						Name:  "B",
						ID:    MustIDBase16(bucketThreeID),
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
		},
		{
			name: "delete buckets using id that does not exist",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						Name:  "A",
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
					},
					{
						Name:  "B",
						ID:    MustIDBase16(bucketThreeID),
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
			args: args{
				ID: "1234567890654321",
			},
			wants: wants{
				err: &influxdb.Error{
					Op:   influxdb.OpDeleteBucket,
					Msg:  "bucket not found",
					Code: influxdb.ENotFound,
				},
				buckets: []*influxdb.Bucket{
					{
						Name:  "A",
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
					},
					{
						Name:  "B",
						ID:    MustIDBase16(bucketThreeID),
						OrgID: MustIDBase16(orgOneID),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.DeleteBucket(ctx, MustIDBase16(tt.args.ID))
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			filter := influxdb.BucketFilter{}
			buckets, _, err := s.FindBuckets(ctx, filter)
			if err != nil {
				t.Fatalf("failed to retrieve buckets: %v", err)
			}

			// remove system buckets
			filteredBuckets := []*influxdb.Bucket{}
			for _, b := range buckets {
				if b.ID > 15 {
					filteredBuckets = append(filteredBuckets, b)
				}
			}

			if diff := cmp.Diff(filteredBuckets, tt.wants.buckets, bucketCmpOptions...); diff != "" {
				t.Errorf("buckets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindBucket testing
func FindBucket(
	init func(BucketFields, *testing.T) (influxdb.BucketService, string, func()),
	t *testing.T,
) {
	type args struct {
		name           string
		organizationID influxdb.ID
	}

	type wants struct {
		bucket *influxdb.Bucket
		err    error
	}

	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "find bucket by name",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "abc",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "xyz",
					},
				},
			},
			args: args{
				name:           "abc",
				organizationID: MustIDBase16(orgOneID),
			},
			wants: wants{
				bucket: &influxdb.Bucket{
					ID:    MustIDBase16(bucketOneID),
					OrgID: MustIDBase16(orgOneID),
					Name:  "abc",
				},
			},
		},
		{
			name: "missing bucket returns error",
			fields: BucketFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{},
			},
			args: args{
				name:           "xyz",
				organizationID: MustIDBase16(orgOneID),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindBucket,
					Msg:  "bucket \"xyz\" not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			filter := influxdb.BucketFilter{}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}
			if tt.args.organizationID.Valid() {
				filter.OrganizationID = &tt.args.organizationID
			}

			bucket, err := s.FindBucket(ctx, filter)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(bucket, tt.wants.bucket, bucketCmpOptions...); diff != "" {
				t.Errorf("buckets are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateBucket testing
func UpdateBucket(
	init func(BucketFields, *testing.T) (influxdb.BucketService, string, func()),
	t *testing.T,
) {
	type args struct {
		name        string
		id          influxdb.ID
		retention   int
		description *string
	}
	type wants struct {
		err    error
		bucket *influxdb.Bucket
	}

	tests := []struct {
		name   string
		fields BucketFields
		args   args
		wants  wants
	}{
		{
			name: "update name",
			fields: BucketFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket1",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket2",
					},
				},
			},
			args: args{
				id:   MustIDBase16(bucketOneID),
				name: "changed",
			},
			wants: wants{
				bucket: &influxdb.Bucket{
					ID:    MustIDBase16(bucketOneID),
					OrgID: MustIDBase16(orgOneID),
					Name:  "changed",
					CRUDLog: influxdb.CRUDLog{
						UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "update name unique",
			fields: BucketFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket1",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket2",
					},
				},
			},
			args: args{
				id:   MustIDBase16(bucketOneID),
				name: "bucket2",
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EConflict,
					Msg:  "bucket name is not unique",
				},
			},
		},
		{
			name: "update retention",
			fields: BucketFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket1",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket2",
					},
				},
			},
			args: args{
				id:        MustIDBase16(bucketOneID),
				retention: 100,
			},
			wants: wants{
				bucket: &influxdb.Bucket{
					ID:              MustIDBase16(bucketOneID),
					OrgID:           MustIDBase16(orgOneID),
					Name:            "bucket1",
					RetentionPeriod: 100 * time.Minute,
					CRUDLog: influxdb.CRUDLog{
						UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "update description",
			fields: BucketFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket1",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket2",
					},
				},
			},
			args: args{
				id:          MustIDBase16(bucketOneID),
				description: stringPtr("desc1"),
			},
			wants: wants{
				bucket: &influxdb.Bucket{
					ID:          MustIDBase16(bucketOneID),
					OrgID:       MustIDBase16(orgOneID),
					Name:        "bucket1",
					Description: "desc1",
					CRUDLog: influxdb.CRUDLog{
						UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "update retention and name",
			fields: BucketFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket1",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket2",
					},
				},
			},
			args: args{
				id:        MustIDBase16(bucketTwoID),
				retention: 101,
				name:      "changed",
			},
			wants: wants{
				bucket: &influxdb.Bucket{
					ID:              MustIDBase16(bucketTwoID),
					OrgID:           MustIDBase16(orgOneID),
					Name:            "changed",
					RetentionPeriod: 101 * time.Minute,
					CRUDLog: influxdb.CRUDLog{
						UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "update retention and same name",
			fields: BucketFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						Name: "theorg",
						ID:   MustIDBase16(orgOneID),
					},
				},
				Buckets: []*influxdb.Bucket{
					{
						ID:    MustIDBase16(bucketOneID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket1",
					},
					{
						ID:    MustIDBase16(bucketTwoID),
						OrgID: MustIDBase16(orgOneID),
						Name:  "bucket2",
					},
				},
			},
			args: args{
				id:        MustIDBase16(bucketTwoID),
				retention: 101,
				name:      "bucket2",
			},
			wants: wants{
				bucket: &influxdb.Bucket{
					ID:              MustIDBase16(bucketTwoID),
					OrgID:           MustIDBase16(orgOneID),
					Name:            "bucket2",
					RetentionPeriod: 101 * time.Minute,
					CRUDLog: influxdb.CRUDLog{
						UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			upd := influxdb.BucketUpdate{}
			if tt.args.name != "" {
				upd.Name = &tt.args.name
			}
			if tt.args.retention != 0 {
				d := time.Duration(tt.args.retention) * time.Minute
				upd.RetentionPeriod = &d
			}

			upd.Description = tt.args.description

			bucket, err := s.UpdateBucket(ctx, tt.args.id, upd)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(bucket, tt.wants.bucket, bucketCmpOptions...); diff != "" {
				t.Errorf("bucket is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
