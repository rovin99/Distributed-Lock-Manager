// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.28.3
// source: lock.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	LockService_ClientInit_FullMethodName           = "/lock.LockService/ClientInit"
	LockService_LockAcquire_FullMethodName          = "/lock.LockService/LockAcquire"
	LockService_LockRelease_FullMethodName          = "/lock.LockService/LockRelease"
	LockService_FileAppend_FullMethodName           = "/lock.LockService/FileAppend"
	LockService_RenewLease_FullMethodName           = "/lock.LockService/RenewLease"
	LockService_UpdateSecondaryState_FullMethodName = "/lock.LockService/UpdateSecondaryState"
	LockService_Ping_FullMethodName                 = "/lock.LockService/Ping"
	LockService_ServerInfo_FullMethodName           = "/lock.LockService/ServerInfo"
	LockService_VerifyFileAccess_FullMethodName     = "/lock.LockService/VerifyFileAccess"
	LockService_ProposePromotion_FullMethodName     = "/lock.LockService/ProposePromotion"
)

// LockServiceClient is the client API for LockService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// The lock service definition
type LockServiceClient interface {
	// Initialize a new client
	ClientInit(ctx context.Context, in *ClientInitArgs, opts ...grpc.CallOption) (*ClientInitResponse, error)
	// Acquire a lock
	LockAcquire(ctx context.Context, in *LockArgs, opts ...grpc.CallOption) (*LockResponse, error)
	// Release a lock
	LockRelease(ctx context.Context, in *LockArgs, opts ...grpc.CallOption) (*LockResponse, error)
	// Append to a file
	FileAppend(ctx context.Context, in *FileArgs, opts ...grpc.CallOption) (*FileResponse, error)
	// Renew lease for a lock
	RenewLease(ctx context.Context, in *LeaseArgs, opts ...grpc.CallOption) (*LeaseResponse, error)
	// Update the secondary server's lock state (Primary -> Secondary)
	UpdateSecondaryState(ctx context.Context, in *ReplicatedState, opts ...grpc.CallOption) (*ReplicationResponse, error)
	// Heartbeat to check if primary is alive (Secondary -> Primary)
	Ping(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error)
	// Server info (for duplicate ID detection)
	ServerInfo(ctx context.Context, in *ServerInfoRequest, opts ...grpc.CallOption) (*ServerInfoResponse, error)
	// Verify file access (to check shared filesystem)
	VerifyFileAccess(ctx context.Context, in *FileAccessRequest, opts ...grpc.CallOption) (*FileAccessResponse, error)
	// Leader election (propose becoming the leader)
	ProposePromotion(ctx context.Context, in *ProposeRequest, opts ...grpc.CallOption) (*ProposeResponse, error)
}

type lockServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewLockServiceClient(cc grpc.ClientConnInterface) LockServiceClient {
	return &lockServiceClient{cc}
}

func (c *lockServiceClient) ClientInit(ctx context.Context, in *ClientInitArgs, opts ...grpc.CallOption) (*ClientInitResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ClientInitResponse)
	err := c.cc.Invoke(ctx, LockService_ClientInit_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lockServiceClient) LockAcquire(ctx context.Context, in *LockArgs, opts ...grpc.CallOption) (*LockResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LockResponse)
	err := c.cc.Invoke(ctx, LockService_LockAcquire_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lockServiceClient) LockRelease(ctx context.Context, in *LockArgs, opts ...grpc.CallOption) (*LockResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LockResponse)
	err := c.cc.Invoke(ctx, LockService_LockRelease_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lockServiceClient) FileAppend(ctx context.Context, in *FileArgs, opts ...grpc.CallOption) (*FileResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(FileResponse)
	err := c.cc.Invoke(ctx, LockService_FileAppend_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lockServiceClient) RenewLease(ctx context.Context, in *LeaseArgs, opts ...grpc.CallOption) (*LeaseResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(LeaseResponse)
	err := c.cc.Invoke(ctx, LockService_RenewLease_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lockServiceClient) UpdateSecondaryState(ctx context.Context, in *ReplicatedState, opts ...grpc.CallOption) (*ReplicationResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ReplicationResponse)
	err := c.cc.Invoke(ctx, LockService_UpdateSecondaryState_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lockServiceClient) Ping(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(HeartbeatResponse)
	err := c.cc.Invoke(ctx, LockService_Ping_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lockServiceClient) ServerInfo(ctx context.Context, in *ServerInfoRequest, opts ...grpc.CallOption) (*ServerInfoResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ServerInfoResponse)
	err := c.cc.Invoke(ctx, LockService_ServerInfo_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lockServiceClient) VerifyFileAccess(ctx context.Context, in *FileAccessRequest, opts ...grpc.CallOption) (*FileAccessResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(FileAccessResponse)
	err := c.cc.Invoke(ctx, LockService_VerifyFileAccess_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *lockServiceClient) ProposePromotion(ctx context.Context, in *ProposeRequest, opts ...grpc.CallOption) (*ProposeResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ProposeResponse)
	err := c.cc.Invoke(ctx, LockService_ProposePromotion_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// LockServiceServer is the server API for LockService service.
// All implementations must embed UnimplementedLockServiceServer
// for forward compatibility.
//
// The lock service definition
type LockServiceServer interface {
	// Initialize a new client
	ClientInit(context.Context, *ClientInitArgs) (*ClientInitResponse, error)
	// Acquire a lock
	LockAcquire(context.Context, *LockArgs) (*LockResponse, error)
	// Release a lock
	LockRelease(context.Context, *LockArgs) (*LockResponse, error)
	// Append to a file
	FileAppend(context.Context, *FileArgs) (*FileResponse, error)
	// Renew lease for a lock
	RenewLease(context.Context, *LeaseArgs) (*LeaseResponse, error)
	// Update the secondary server's lock state (Primary -> Secondary)
	UpdateSecondaryState(context.Context, *ReplicatedState) (*ReplicationResponse, error)
	// Heartbeat to check if primary is alive (Secondary -> Primary)
	Ping(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	// Server info (for duplicate ID detection)
	ServerInfo(context.Context, *ServerInfoRequest) (*ServerInfoResponse, error)
	// Verify file access (to check shared filesystem)
	VerifyFileAccess(context.Context, *FileAccessRequest) (*FileAccessResponse, error)
	// Leader election (propose becoming the leader)
	ProposePromotion(context.Context, *ProposeRequest) (*ProposeResponse, error)
	mustEmbedUnimplementedLockServiceServer()
}

// UnimplementedLockServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedLockServiceServer struct{}

func (UnimplementedLockServiceServer) ClientInit(context.Context, *ClientInitArgs) (*ClientInitResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ClientInit not implemented")
}
func (UnimplementedLockServiceServer) LockAcquire(context.Context, *LockArgs) (*LockResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LockAcquire not implemented")
}
func (UnimplementedLockServiceServer) LockRelease(context.Context, *LockArgs) (*LockResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method LockRelease not implemented")
}
func (UnimplementedLockServiceServer) FileAppend(context.Context, *FileArgs) (*FileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method FileAppend not implemented")
}
func (UnimplementedLockServiceServer) RenewLease(context.Context, *LeaseArgs) (*LeaseResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RenewLease not implemented")
}
func (UnimplementedLockServiceServer) UpdateSecondaryState(context.Context, *ReplicatedState) (*ReplicationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UpdateSecondaryState not implemented")
}
func (UnimplementedLockServiceServer) Ping(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Ping not implemented")
}
func (UnimplementedLockServiceServer) ServerInfo(context.Context, *ServerInfoRequest) (*ServerInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ServerInfo not implemented")
}
func (UnimplementedLockServiceServer) VerifyFileAccess(context.Context, *FileAccessRequest) (*FileAccessResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method VerifyFileAccess not implemented")
}
func (UnimplementedLockServiceServer) ProposePromotion(context.Context, *ProposeRequest) (*ProposeResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ProposePromotion not implemented")
}
func (UnimplementedLockServiceServer) mustEmbedUnimplementedLockServiceServer() {}
func (UnimplementedLockServiceServer) testEmbeddedByValue()                     {}

// UnsafeLockServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to LockServiceServer will
// result in compilation errors.
type UnsafeLockServiceServer interface {
	mustEmbedUnimplementedLockServiceServer()
}

func RegisterLockServiceServer(s grpc.ServiceRegistrar, srv LockServiceServer) {
	// If the following call pancis, it indicates UnimplementedLockServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&LockService_ServiceDesc, srv)
}

func _LockService_ClientInit_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ClientInitArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LockServiceServer).ClientInit(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LockService_ClientInit_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LockServiceServer).ClientInit(ctx, req.(*ClientInitArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _LockService_LockAcquire_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LockArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LockServiceServer).LockAcquire(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LockService_LockAcquire_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LockServiceServer).LockAcquire(ctx, req.(*LockArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _LockService_LockRelease_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LockArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LockServiceServer).LockRelease(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LockService_LockRelease_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LockServiceServer).LockRelease(ctx, req.(*LockArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _LockService_FileAppend_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FileArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LockServiceServer).FileAppend(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LockService_FileAppend_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LockServiceServer).FileAppend(ctx, req.(*FileArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _LockService_RenewLease_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(LeaseArgs)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LockServiceServer).RenewLease(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LockService_RenewLease_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LockServiceServer).RenewLease(ctx, req.(*LeaseArgs))
	}
	return interceptor(ctx, in, info, handler)
}

func _LockService_UpdateSecondaryState_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ReplicatedState)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LockServiceServer).UpdateSecondaryState(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LockService_UpdateSecondaryState_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LockServiceServer).UpdateSecondaryState(ctx, req.(*ReplicatedState))
	}
	return interceptor(ctx, in, info, handler)
}

func _LockService_Ping_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LockServiceServer).Ping(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LockService_Ping_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LockServiceServer).Ping(ctx, req.(*HeartbeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LockService_ServerInfo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ServerInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LockServiceServer).ServerInfo(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LockService_ServerInfo_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LockServiceServer).ServerInfo(ctx, req.(*ServerInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LockService_VerifyFileAccess_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FileAccessRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LockServiceServer).VerifyFileAccess(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LockService_VerifyFileAccess_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LockServiceServer).VerifyFileAccess(ctx, req.(*FileAccessRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _LockService_ProposePromotion_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProposeRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(LockServiceServer).ProposePromotion(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: LockService_ProposePromotion_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(LockServiceServer).ProposePromotion(ctx, req.(*ProposeRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// LockService_ServiceDesc is the grpc.ServiceDesc for LockService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var LockService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "lock.LockService",
	HandlerType: (*LockServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ClientInit",
			Handler:    _LockService_ClientInit_Handler,
		},
		{
			MethodName: "LockAcquire",
			Handler:    _LockService_LockAcquire_Handler,
		},
		{
			MethodName: "LockRelease",
			Handler:    _LockService_LockRelease_Handler,
		},
		{
			MethodName: "FileAppend",
			Handler:    _LockService_FileAppend_Handler,
		},
		{
			MethodName: "RenewLease",
			Handler:    _LockService_RenewLease_Handler,
		},
		{
			MethodName: "UpdateSecondaryState",
			Handler:    _LockService_UpdateSecondaryState_Handler,
		},
		{
			MethodName: "Ping",
			Handler:    _LockService_Ping_Handler,
		},
		{
			MethodName: "ServerInfo",
			Handler:    _LockService_ServerInfo_Handler,
		},
		{
			MethodName: "VerifyFileAccess",
			Handler:    _LockService_VerifyFileAccess_Handler,
		},
		{
			MethodName: "ProposePromotion",
			Handler:    _LockService_ProposePromotion_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "lock.proto",
}
