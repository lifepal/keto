package relationtuple

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	rts "github.com/ory/keto/proto/ory/keto/relation_tuples/v1alpha2"

	"github.com/ory/herodot"

	"github.com/pkg/errors"

	"github.com/julienschmidt/httprouter"

	"github.com/ory/keto/internal/x"
)

var (
	_ rts.ReadServiceServer = (*handler)(nil)
	_                       = (*getRelationsParams)(nil)
)

func (h *handler) ListRelationTuples(ctx context.Context, req *rts.ListRelationTuplesRequest) (*rts.ListRelationTuplesResponse, error) {
	if req.Query == nil {
		return nil, errors.New("invalid request")
	}

	q, err := (&RelationQuery{}).FromProto(req.Query)
	if err != nil {
		return nil, err
	}

	rels, nextPage, err := h.d.RelationTupleManager().GetRelationTuples(ctx, q,
		x.WithSize(int(req.PageSize)),
		x.WithToken(req.PageToken),
	)
	if err != nil {
		return nil, err
	}

	resp := &rts.ListRelationTuplesResponse{
		RelationTuples: make([]*rts.RelationTuple, len(rels)),
		NextPageToken:  nextPage,
	}
	for i, r := range rels {
		resp.RelationTuples[i] = r.ToProto()
	}

	return resp, nil
}

// swagger:parameters getRelationTuples
type getRelationsParams struct {
	// Namespace of the Relation Tuple
	//
	// in: query
	Namespace string `json:"namespace"`

	// Object of the Relation Tuple
	//
	// in: query
	Object string `json:"object"`

	// Relation of the Relation Tuple
	//
	// in: query
	Relation string `json:"relation"`

	// SubjectID of the Relation Tuple
	//
	// in: query
	// Either subject_set.* or subject_id are required.
	SubjectID string `json:"subject_id"`

	// Namespace of the Subject Set
	//
	// in: query
	// Either subject_set.* or subject_id are required.
	SNamespace string `json:"subject_set.namespace"`

	// Object of the Subject Set
	//
	// in: query
	// Either subject_set.* or subject_id are required.
	SObject string `json:"subject_set.object"`

	// Relation of the Subject Set
	//
	// in: query
	// Either subject_set.* or subject_id are required.
	SRelation string `json:"subject_set.relation"`

	// swagger:allOf
	x.PaginationOptions
}

// swagger:route GET /relation-tuples read getRelationTuples
//
// Query relation tuples
//
// Get all relation tuples that match the query. Only the namespace field is required.
//
//     Consumes:
//     -  application/x-www-form-urlencoded
//
//     Produces:
//     - application/json
//
//     Schemes: http, https
//
//     Responses:
//       200: getRelationTuplesResponse
//       404: genericError
//       500: genericError
func (h *handler) getRelations(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	q := r.URL.Query()
	query, err := (&RelationQuery{}).FromURLQuery(q)
	if err != nil {
		h.d.Writer().WriteError(w, r, herodot.ErrBadRequest.WithError(err.Error()))
		return
	}
	resp, err := h.fetchRelationship(q, query)
	if err != nil {
		h.d.Writer().WriteError(w, r, herodot.ErrBadRequest.WithError(err.Error()))
		return
	}

	if len(resp.RelationTuples) == 0 {
		h.d.Writer().Write(w, r, resp)
		return
	}

	if userPermissions := q.Get("up"); userPermissions == "all" {
		// get permission role
		resp, err = h.findPermissions(resp)
		if err != nil {
			h.d.Writer().WriteError(w, r, herodot.ErrBadRequest.WithError(err.Error()))
			return
		}
		// get all role permissions
		resp, err = h.findPermissions(resp)
		if err != nil {
			h.d.Writer().WriteError(w, r, herodot.ErrBadRequest.WithError(err.Error()))
			return
		}
	}
	h.d.Writer().Write(w, r, resp)
}

func (h *handler) findPermissions(resp *GetResponse) (*GetResponse, error) {
	var result = new(GetResponse)
	for _, r := range resp.RelationTuples {
		urlValues := url.Values{
			"subject_set.namespace": {r.Namespace},
			"subject_set.relation":  {r.Relation},
			"subject_set.object":    {r.Object},
		}
		query, err := (&RelationQuery{}).FromURLQuery(urlValues)
		if err != nil {
			return nil, err
		}
		rel, err := h.fetchRelationship(urlValues, query)
		if err != nil {
			return nil, err
		}
		if len(rel.RelationTuples) == 0 {
			result.RelationTuples = append(result.RelationTuples, r)
			continue
		}

		result.RelationTuples = append(result.RelationTuples, rel.RelationTuples...)
	}
	return result, nil
}

func (h *handler) fetchRelationship(q url.Values, query *RelationQuery) (*GetResponse, error) {
	var arrQuery []*RelationQuery
	if query.SubjectSet != nil {
		if subjectSetArr := strings.Split(query.SubjectSet.Object, ","); len(subjectSetArr) > 0 {
			for _, val := range subjectSetArr {
				var t = new(RelationQuery)
				t.Namespace = query.Namespace
				t.Object = query.Object
				t.Relation = query.Relation
				t.SubjectID = query.SubjectID
				t.SubjectSet = new(SubjectSet)
				t.SubjectSet.Namespace = query.SubjectSet.Namespace
				t.SubjectSet.Relation = query.SubjectSet.Relation
				t.SubjectSet.Object = val
				arrQuery = append(arrQuery, t)
			}
		}
	}

	var paginationOpts []x.PaginationOptionSetter
	if pageToken := q.Get("page_token"); pageToken != "" {
		paginationOpts = append(paginationOpts, x.WithToken(pageToken))
	}

	if pageSize := q.Get("page_size"); pageSize != "" {
		s, err := strconv.ParseInt(pageSize, 0, 0)
		if err != nil {
			return nil, err
		}
		paginationOpts = append(paginationOpts, x.WithSize(int(s)))
	} else {
		// set default pagination
		paginationOpts = append(paginationOpts, x.WithSize(1000))
	}

	if len(arrQuery) == 0 {
		rels, nextPage, err := h.d.RelationTupleManager().GetRelationTuples(context.Background(), query, paginationOpts...)
		if err != nil {
			return nil, err
		}
		resp := &GetResponse{
			RelationTuples: rels,
			NextPageToken:  nextPage,
		}
		return resp, nil
	}

	var rels []*InternalRelationTuple
	for _, q := range arrQuery {
		tmp, _, err := h.d.RelationTupleManager().GetRelationTuples(context.Background(), q, paginationOpts...)
		if err != nil {
			return nil, err
		}
		rels = append(rels, tmp...)
	}
	resp := &GetResponse{
		RelationTuples: rels,
		NextPageToken:  "",
	}
	return resp, nil
}
