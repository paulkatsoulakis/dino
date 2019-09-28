// The metadata server will read commands and write responses, but not in a
// strict request-response fashion. The responses will include results of
// commands from a certain client but will include results of commands from
// other clients. For example, the client might send "GET name" while receiving
// "PUT 9 age fourteen" (resulting from some other client's PUT) and then "PUT 4
// name Robert". In other words, the metadata server streams state updates
// coming from all clients. (The functionality is mostly in this module's
// metadata/server package.)
package main // import "github.com/nicolagi/dino/cmd/metadataserver"
