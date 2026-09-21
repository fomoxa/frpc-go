package frpc

import (
	"slices"
)

type Identity struct {
	Principal   string
	Permissions []string
}

func NewIdentity(principal string, permissions ...string) *Identity {
	sorted := slices.Clone(permissions)
	slices.Sort(sorted)
	return &Identity{Principal: principal, Permissions: slices.Compact(sorted)}
}

func (i *Identity) Permits(permission string) bool {
	_, found := slices.BinarySearch(i.Permissions, permission)
	return found
}

func (i *Identity) NarrowedBy(other *Identity) []string {
	narrowed := []string{}
	for _, permission := range i.Permissions {
		if other.Permits(permission) {
			narrowed = append(narrowed, permission)
		}
	}
	return narrowed
}

func (i *Identity) String() string { return i.Principal }

type TokenVerifier func(token string) *Identity
