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
	"github.com/influxdata/influxdb/rand"
)

const (
	orgOneID = "020f755c3c083000"
	orgTwoID = "020f755c3c083001"
)

var organizationCmpOptions = cmp.Options{
	cmp.Comparer(func(x, y []byte) bool {
		return bytes.Equal(x, y)
	}),
	cmp.Transformer("Sort", func(in []*influxdb.Organization) []*influxdb.Organization {
		out := append([]*influxdb.Organization(nil), in...) // Copy input to avoid mutating it
		sort.Slice(out, func(i, j int) bool {
			return out[i].ID.String() > out[j].ID.String()
		})
		return out
	}),
}

// OrganizationFields will include the IDGenerator, and organizations
type OrganizationFields struct {
	IDGenerator   influxdb.IDGenerator
	Organizations []*influxdb.Organization
	TimeGenerator influxdb.TimeGenerator
	OrgBucketIDs  influxdb.IDGenerator
}

// OrganizationService tests all the service functions.
func OrganizationService(
	init func(OrganizationFields, *testing.T) (influxdb.OrganizationService, string, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(OrganizationFields, *testing.T) (influxdb.OrganizationService, string, func()),
			t *testing.T)
	}{
		{
			name: "CreateOrganization",
			fn:   CreateOrganization,
		},
		{
			name: "FindOrganizationByID",
			fn:   FindOrganizationByID,
		},
		{
			name: "FindOrganizations",
			fn:   FindOrganizations,
		},
		{
			name: "DeleteOrganization",
			fn:   DeleteOrganization,
		},
		{
			name: "FindOrganization",
			fn:   FindOrganization,
		},
		{
			name: "UpdateOrganization",
			fn:   UpdateOrganization,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// CreateOrganization testing
func CreateOrganization(
	init func(OrganizationFields, *testing.T) (influxdb.OrganizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		organization *influxdb.Organization
	}
	type wants struct {
		err           error
		organizations []*influxdb.Organization
	}

	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "create organizations with empty set",
			fields: OrganizationFields{
				IDGenerator:   mock.NewIDGenerator(orgOneID, t),
				OrgBucketIDs:  mock.NewIDGenerator(orgOneID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{},
			},
			args: args{
				organization: &influxdb.Organization{
					Name:        "name1",
					ID:          MustIDBase16(orgOneID),
					Description: "desc1",
				},
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						Name:        "name1",
						ID:          MustIDBase16(orgOneID),
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
			name: "basic create organization",
			fields: OrganizationFields{
				IDGenerator:   mock.NewIDGenerator(orgTwoID, t),
				OrgBucketIDs:  mock.NewIDGenerator(orgTwoID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
			},
			args: args{
				organization: &influxdb.Organization{
					ID:   MustIDBase16(orgTwoID),
					Name: "organization2",
				},
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "organization2",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
				},
			},
		},
		{
			name: "empty name",
			fields: OrganizationFields{
				IDGenerator:   mock.NewIDGenerator(orgTwoID, t),
				OrgBucketIDs:  mock.NewIDGenerator(orgTwoID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
			},
			args: args{
				organization: &influxdb.Organization{
					ID: MustIDBase16(orgOneID),
				},
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
				err: influxdb.ErrOrgNameisEmpty,
			},
		},
		{
			name: "name only have spaces",
			fields: OrganizationFields{
				IDGenerator:   mock.NewIDGenerator(orgTwoID, t),
				OrgBucketIDs:  mock.NewIDGenerator(orgTwoID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
			},
			args: args{
				organization: &influxdb.Organization{
					ID:   MustIDBase16(orgOneID),
					Name: "  ",
				},
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
				err: influxdb.ErrOrgNameisEmpty,
			},
		},
		{
			name: "names should be unique",
			fields: OrganizationFields{
				IDGenerator:   mock.NewIDGenerator(orgTwoID, t),
				OrgBucketIDs:  mock.NewIDGenerator(orgTwoID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
			},
			args: args{
				organization: &influxdb.Organization{
					ID:   MustIDBase16(orgOneID),
					Name: "organization1",
				},
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
				err: &influxdb.Error{
					Code: influxdb.EConflict,
					Op:   influxdb.OpCreateOrganization,
					Msg:  "organization with name organization1 already exists",
				},
			},
		},
		{
			name: "create organization with no id",
			fields: OrganizationFields{
				IDGenerator:   mock.NewIDGenerator(orgTwoID, t),
				OrgBucketIDs:  mock.NewIDGenerator(orgTwoID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
			},
			args: args{
				organization: &influxdb.Organization{
					Name: "organization2",
				},
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "organization2",
						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
					},
				},
			},
		},
		{
			name: "names should be unique",
			fields: OrganizationFields{
				IDGenerator:   mock.NewIDGenerator(orgTwoID, t),
				OrgBucketIDs:  mock.NewIDGenerator(orgTwoID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
			},
			args: args{
				organization: &influxdb.Organization{
					ID:   MustIDBase16(orgOneID),
					Name: "organization1",
				},
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
				err: &influxdb.Error{
					Code: influxdb.EConflict,
					Op:   influxdb.OpCreateOrganization,
					Msg:  "organization with name organization1 already exists",
				},
			},
		},
		{
			name: "ids should be unique",
			fields: OrganizationFields{
				IDGenerator:   mock.NewIDGenerator(orgOneID, t),
				OrgBucketIDs:  mock.NewIDGenerator(orgOneID, t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
			},
			args: args{
				organization: &influxdb.Organization{
					ID:   MustIDBase16(orgOneID),
					Name: "organization2",
				},
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
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
			fields: OrganizationFields{
				IDGenerator:   mock.NewIDGenerator("000000000000000a", t),
				OrgBucketIDs:  mock.NewIDGenerator("000000000000000a", t),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
			},
			args: args{
				organization: &influxdb.Organization{
					ID:   MustIDBase16(orgOneID),
					Name: "organization2",
				},
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
				},
				err: &influxdb.Error{
					Code: influxdb.EInternal,
					Msg:  fmt.Sprintf("unable to generate valid id"),
				},
			},
		},
		{
			name: "randomly generted org ids should not have commas, spaces, or backslashes",
			fields: OrganizationFields{
				OrgBucketIDs:  rand.NewOrgBucketID(42),
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{},
			},
			args: args{
				organization: &influxdb.Organization{
					Name: "o1",
				},
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16("afbf64b1967f8c53"),
						Name: "o1",

						CRUDLog: influxdb.CRUDLog{
							CreatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
							UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
						},
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
			err := s.CreateOrganization(ctx, tt.args.organization)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			// Delete only newly created organizations
			// if tt.args.organization.ID != nil {
			defer s.DeleteOrganization(ctx, tt.args.organization.ID)
			// }

			organizations, _, err := s.FindOrganizations(ctx, influxdb.OrganizationFilter{})
			diffPlatformErrors(tt.name, err, nil, opPrefix, t)
			if diff := cmp.Diff(organizations, tt.wants.organizations, organizationCmpOptions...); diff != "" {
				t.Errorf("organizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindOrganizationByID testing
func FindOrganizationByID(
	init func(OrganizationFields, *testing.T) (influxdb.OrganizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		id influxdb.ID
	}
	type wants struct {
		err          error
		organization *influxdb.Organization
	}

	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "basic find organization by id",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "organization2",
					},
				},
			},
			args: args{
				id: MustIDBase16(orgTwoID),
			},
			wants: wants{
				organization: &influxdb.Organization{
					ID:   MustIDBase16(orgTwoID),
					Name: "organization2",
				},
			},
		},
		{
			name: "didn't find organization by id",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "organization2",
					},
				},
			},
			args: args{
				id: MustIDBase16(threeID),
			},
			wants: wants{
				organization: nil,
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindOrganizationByID,
					Msg:  "organization not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			organization, err := s.FindOrganizationByID(ctx, tt.args.id)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(organization, tt.wants.organization, organizationCmpOptions...); diff != "" {
				t.Errorf("organization is different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindOrganizations testing
func FindOrganizations(
	init func(OrganizationFields, *testing.T) (influxdb.OrganizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID   influxdb.ID
		name string
	}

	type wants struct {
		organizations []*influxdb.Organization
		err           error
	}
	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "find all organizations",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "abc",
					},
					{
						ID:          MustIDBase16(orgTwoID),
						Name:        "xyz",
						Description: "desc xyz",
					},
				},
			},
			args: args{},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "abc",
					},
					{
						ID:          MustIDBase16(orgTwoID),
						Name:        "xyz",
						Description: "desc xyz",
					},
				},
			},
		},
		{
			name: "find organization by id",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				ID: MustIDBase16(orgTwoID),
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "xyz",
					},
				},
			},
		},
		{
			name: "find organization by name",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				name: "xyz",
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "xyz",
					},
				},
			},
		},
		{
			name: "find organization by id not exists",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				ID: MustIDBase16(threeID),
			},
			wants: wants{
				organizations: []*influxdb.Organization{},
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindOrganizations,
					Msg:  "organization not found",
				},
			},
		},
		{
			name: "find organization by name not exists",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				name: "na",
			},
			wants: wants{
				organizations: []*influxdb.Organization{},
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindOrganizations,
					Msg:  "organization name \"na\" not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()

			filter := influxdb.OrganizationFilter{}
			if tt.args.ID.Valid() {
				filter.ID = &tt.args.ID
			}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}

			organizations, _, err := s.FindOrganizations(ctx, filter)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(organizations, tt.wants.organizations, organizationCmpOptions...); diff != "" {
				t.Errorf("organizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// DeleteOrganization testing
func DeleteOrganization(
	init func(OrganizationFields, *testing.T) (influxdb.OrganizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		ID string
	}
	type wants struct {
		err           error
		organizations []*influxdb.Organization
	}

	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "delete organizations using exist id",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "orgA",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "orgB",
						ID:   MustIDBase16(orgTwoID),
					},
				},
			},
			args: args{
				ID: orgOneID,
			},
			wants: wants{
				organizations: []*influxdb.Organization{
					{
						Name: "orgB",
						ID:   MustIDBase16(orgTwoID),
					},
				},
			},
		},
		{
			name: "delete organizations using id that does not exist",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						Name: "orgA",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "orgB",
						ID:   MustIDBase16(orgTwoID),
					},
				},
			},
			args: args{
				ID: "1234567890654321",
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpDeleteOrganization,
					Msg:  "organization not found",
				},
				organizations: []*influxdb.Organization{
					{
						Name: "orgA",
						ID:   MustIDBase16(orgOneID),
					},
					{
						Name: "orgB",
						ID:   MustIDBase16(orgTwoID),
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
			err := s.DeleteOrganization(ctx, MustIDBase16(tt.args.ID))
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			filter := influxdb.OrganizationFilter{}
			organizations, _, err := s.FindOrganizations(ctx, filter)
			diffPlatformErrors(tt.name, err, nil, opPrefix, t)

			if diff := cmp.Diff(organizations, tt.wants.organizations, organizationCmpOptions...); diff != "" {
				t.Errorf("organizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// FindOrganization testing
func FindOrganization(
	init func(OrganizationFields, *testing.T) (influxdb.OrganizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		name string
		id   influxdb.ID
	}

	type wants struct {
		organization *influxdb.Organization
		err          error
	}

	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "find organization by name",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "abc",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "xyz",
					},
				},
			},
			args: args{
				name: "abc",
			},
			wants: wants{
				organization: &influxdb.Organization{
					ID:   MustIDBase16(orgOneID),
					Name: "abc",
				},
			},
		},
		{
			name: "find organization in which no name filter matches should return no org",
			args: args{
				name: "unknown",
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindOrganization,
					Msg:  "organization name \"unknown\" not found",
				},
			},
		},
		{
			name: "find organization in which no id filter matches should return no org",
			args: args{
				id: influxdb.ID(3),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Msg:  "organization not found",
				},
			},
		},
		{
			name: "find organization no filter is set returns an error about filters not provided",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "o1",
					},
				},
			},
			wants: wants{
				err: influxdb.ErrInvalidOrgFilter,
			},
		},
		{
			name: "missing organization returns error",
			fields: OrganizationFields{
				Organizations: []*influxdb.Organization{},
			},
			args: args{
				name: "abc",
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpFindOrganization,
					Msg:  "organization name \"abc\" not found",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, opPrefix, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			filter := influxdb.OrganizationFilter{}
			if tt.args.name != "" {
				filter.Name = &tt.args.name
			}
			if tt.args.id != influxdb.InvalidID() {
				filter.ID = &tt.args.id
			}

			organization, err := s.FindOrganization(ctx, filter)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(organization, tt.wants.organization, organizationCmpOptions...); diff != "" {
				t.Errorf("organizations are different -got/+want\ndiff %s", diff)
			}
		})
	}
}

// UpdateOrganization testing
func UpdateOrganization(
	init func(OrganizationFields, *testing.T) (influxdb.OrganizationService, string, func()),
	t *testing.T,
) {
	type args struct {
		id          influxdb.ID
		name        *string
		description *string
	}
	type wants struct {
		err          error
		organization *influxdb.Organization
	}

	tests := []struct {
		name   string
		fields OrganizationFields
		args   args
		wants  wants
	}{
		{
			name: "update id not exists",
			fields: OrganizationFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "organization2",
					},
				},
			},
			args: args{
				id:   MustIDBase16(threeID),
				name: strPtr("changed"),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.ENotFound,
					Op:   influxdb.OpUpdateOrganization,
					Msg:  "organization not found",
				},
			},
		},
		{
			name: "update name",
			fields: OrganizationFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "organization2",
					},
				},
			},
			args: args{
				id:   MustIDBase16(orgOneID),
				name: strPtr("changed"),
			},
			wants: wants{
				organization: &influxdb.Organization{
					ID:   MustIDBase16(orgOneID),
					Name: "changed",
					CRUDLog: influxdb.CRUDLog{
						UpdatedAt: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC),
					},
				},
			},
		},
		{
			name: "update name not unique",
			fields: OrganizationFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "organization2",
					},
				},
			},
			args: args{
				id:   MustIDBase16(orgOneID),
				name: strPtr("organization2"),
			},
			wants: wants{
				err: &influxdb.Error{
					Code: influxdb.EConflict,
					Op:   influxdb.OpUpdateOrganization,
					Msg:  "organization with name organization2 already exists",
				},
			},
		},
		{
			name: "update name is empty",
			fields: OrganizationFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "organization2",
					},
				},
			},
			args: args{
				id:   MustIDBase16(orgOneID),
				name: strPtr(""),
			},
			wants: wants{
				err: influxdb.ErrOrgNameisEmpty,
			},
		},
		{
			name: "update name only has space",
			fields: OrganizationFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:   MustIDBase16(orgOneID),
						Name: "organization1",
					},
					{
						ID:   MustIDBase16(orgTwoID),
						Name: "organization2",
					},
				},
			},
			args: args{
				id:   MustIDBase16(orgOneID),
				name: strPtr("            "),
			},
			wants: wants{
				err: influxdb.ErrOrgNameisEmpty,
			},
		},
		{
			name: "update description",
			fields: OrganizationFields{
				TimeGenerator: mock.TimeGenerator{FakeValue: time.Date(2006, 5, 4, 1, 2, 3, 0, time.UTC)},
				Organizations: []*influxdb.Organization{
					{
						ID:          MustIDBase16(orgOneID),
						Name:        "organization1",
						Description: "organization1 description",
					},
					{
						ID:          MustIDBase16(orgTwoID),
						Name:        "organization2",
						Description: "organization2 description",
					},
				},
			},
			args: args{
				id:          MustIDBase16(orgOneID),
				description: strPtr("changed"),
			},
			wants: wants{
				organization: &influxdb.Organization{
					ID:          MustIDBase16(orgOneID),
					Name:        "organization1",
					Description: "changed",
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

			upd := influxdb.OrganizationUpdate{}
			upd.Name = tt.args.name
			upd.Description = tt.args.description

			organization, err := s.UpdateOrganization(ctx, tt.args.id, upd)
			diffPlatformErrors(tt.name, err, tt.wants.err, opPrefix, t)

			if diff := cmp.Diff(organization, tt.wants.organization, organizationCmpOptions...); diff != "" {
				t.Errorf("organization is different -got/+want\ndiff %s", diff)
			}
		})
	}
}
