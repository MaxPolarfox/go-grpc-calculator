package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"styding/grpc-go-calculator/calculatorpb"
	"io"
	"log"
	"math"
	"net"
)

type service struct {}

func (s *service) AddTwoNums(ctx context.Context, request *calculatorpb.AddTwoNumsRequest) (
	*calculatorpb.AddTwoNumsResponse, error) {
	numOne := request.GetArguments().GetNumOne()
	numTwo := request.GetArguments().GetNumTwo()
	response := &calculatorpb.AddTwoNumsResponse{Result: numOne + numTwo}

	return response, nil
}

func (s *service) DecomposePrimeNumber(req *calculatorpb.PrimeNumberRequest,
	stream calculatorpb.Calculator_DecomposePrimeNumberServer) error {
	fmt.Printf("DecomposePrimeNumber function was invoked with %#v\n", req)
	N := req.GetNumber()

	var k int32 = 2

	for N > 1 {
		if N % k == 0 {

			result := &calculatorpb.PrimeNumberResponse{Number: N}

			stream.Send(result)

			N = N / k
		} else {
			k = k + 1
		}
	}

	return nil
}

func (s *service) AverageSum(stream calculatorpb.Calculator_AverageSumServer) error {
	fmt.Println("AverageSum function was invoked")
	var total float64 = 0
	var numRequests float64 = 0
	for {
		req, err := stream.Recv()
		if err != nil {
			// We are done receiving data
			if err == io.EOF {
				res := &calculatorpb.AverageSumResponse{Result: total / numRequests}
				return stream.SendAndClose(res)
			}
			log.Fatalf("Error while recieving stream: %v", err)
			return err
		}
		total += req.Number
		numRequests++
	}

	return nil
}

func (s *service) SquareRoot(ctx context.Context, req *calculatorpb.SquareRootRequest) (
	*calculatorpb.SquareRootResponse, error)  {
	fmt.Printf("SquareRoot function was invoked with %v", req)

	number := req.GetNumber()

	if number < 0 {
		return nil, status.Error(codes.InvalidArgument,
			fmt.Sprintf("Received Negative number: %v", number))
	}

	return &calculatorpb.SquareRootResponse{
		Result: math.Sqrt(float64(number)),
	}, nil
}

func (s *service) FindMaximum(stream calculatorpb.Calculator_FindMaximumServer) error  {
	fmt.Println("FindMaximum function was invoked")

	var max int32 = math.MinInt32

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}

			fmt.Errorf("Errror while reading steam: %v", err)
			return err
		}

		if max < req.Number {
			max = req.Number
		}

		err = stream.Send(&calculatorpb.FindMaximumResponse{Result: max})
		if err != nil {
			log.Fatalf("Error while sending to client stream: %v", err)
			return err
		}
	}

	return nil
}

func main() {

	lis, err := net.Listen("tcp", "0.0.0.0:50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	serverImpl := service{}
	calculatorpb.RegisterCalculatorServer(s, &serverImpl)

	fmt.Println("Calculator Server runs on: ", "50051")

	if err := s.Serve(lis); err != nil {
		if err != nil {
			log.Fatalf("Failed to serve: %v", err)
		}
	}
}
