package main

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"styding/grpc-go-calculator/calculatorpb"
	"time"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("couldn't connect: %v", err)
	}

	defer conn.Close()

	c := calculatorpb.NewCalculatorClient(conn)

	//doUnary(c)
	//doServerStreaming(c)
	//doClientSteaming(c)
	//doBiDiSteaming(c)
	//doErrorHandling(c, -81)

	doUnaryWithDeadline(c, 5*time.Second) // should complete
	doUnaryWithDeadline(c, 1*time.Second) // should timeout

}

// doUnary sends single request to the server and receives just one response
func doUnary(c calculatorpb.CalculatorClient) {
	fmt.Println("Started to do Unary RPC...")
	req := &calculatorpb.AddTwoNumsRequest{
		Arguments: &calculatorpb.Arguments{
			NumOne: 9,
			NumTwo: 6,
		},
	}

	res, err := c.AddTwoNums(context.Background(), req)
	if err != nil {
		log.Fatalf("Error while calling Calculator RPC: ", err)
	}

	fmt.Println("Response from Calculator: ", res.Result)
}

// doServerStreaming sends single request to the server and receives multiple responses
func doServerStreaming(c calculatorpb.CalculatorClient) {
	fmt.Println("Started to do Server Streaming RPC...")

	req := &calculatorpb.PrimeNumberRequest{Number: 120}

	stream, err := c.DecomposePrimeNumber(context.Background(), req)
	if err != nil {
		log.Fatalf("Error while calling DecomposePrimeNumber RPC: %v", err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				log.Println("Done.")
				// we've reached the end of the stream
				break
			}
			log.Fatalf("Error while reading stream: %v", err)
		}

		log.Printf("Response from DecomposePrimeNumber: %v", msg.GetNumber())
	}
}

// doClientSteaming sends multiple requests to the server and receives a single response
func doClientSteaming(c calculatorpb.CalculatorClient) {
	log.Println("Starting to doClientSteaming RPC...")

	stream, err := c.AverageSum(context.Background())
	if err != nil {
		log.Fatalf("Error while calling AverageSum RPC: %v", err)
	}

	requests := []*calculatorpb.AverageSumRequest{
		{
			Number: 1,
		},
		{
			Number: 17,
		},
		{
			Number: 9,
		},
		{
			Number: 56,
		},
	}

	for _, req := range requests {
		fmt.Println("Sending Request: %v", req)

		err := stream.Send(req)
		if err != nil {
			log.Fatalf("Error while sending stream: %v", err)
		}

		time.Sleep(100 * time.Millisecond)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error while reading stream: %v", err)
	}

	log.Printf("Response from AverageSum: %v", res.Result)
}

// doBiDiSteaming sends multiple requests to the server and receives multiple responses (using goroutine)
func doBiDiSteaming(c calculatorpb.CalculatorClient) {
	fmt.Println("Started to do BiDi Steaming RPC...")

	// create stream
	stream, err := c.FindMaximum(context.Background())
	if err != nil {
		log.Fatalf("Error while calling AverageSum RPC: %v", err)
	}

	waitCh := make(chan struct{})

	// send bunch of messages to client
	nums := []int32{-1, 3, 2, 5, 9, 1}
	go func() {
		for _, num := range nums {
			log.Printf("Sending req: %d\n", num)
			err := stream.Send(&calculatorpb.FindMaximumRequest{Number: num})
			if err != nil {
				log.Fatalf("Error while sending to client: %v", err)
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		stream.CloseSend()
	}()

	// receive bunch of messages from client
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					fmt.Println("Done")
					close(waitCh)
					break
				}
				log.Fatalf("Error while  while recieving stream: %v", err)
				close(waitCh)
				break
			}
			fmt.Printf("Received: %d\n", res.GetResult())
		}
	}()

	// block until everything is done
	<-waitCh
}

// doErrorHandling shows a good practice for error handling
func doErrorHandling(c calculatorpb.CalculatorClient, number int32) {
	fmt.Println("Started to do doErrorHandling RPC...")

	res, err := c.SquareRoot(context.Background(), &calculatorpb.SquareRootRequest{Number: number})
	if err != nil {
		if resErr, ok := status.FromError(err); ok {
			fmt.Println("Code", resErr.Code(), "Err", resErr.Message())
			if resErr.Code() == codes.InvalidArgument {
				log.Println("Negative numbers are not allowed")
			}
		} else {
			log.Fatalf("Big Error calling SquareRoot RPC: %v", err)
		}
	}

	fmt.Printf("Response of square root of %d: %f", number, res.GetResult())
}

func doUnaryWithDeadline(c calculatorpb.CalculatorClient, timeout time.Duration) {
	fmt.Println("Started to do Unary With Deadline RPC...")

	req := &calculatorpb.MultiplyWithDeadlineRequest{
		Arguments: &calculatorpb.Arguments{NumOne: 21, NumTwo: 99},
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res, err := c.MultiplyWithDeadline(ctx, req)
	if err != nil {
		if statusErr, ok := status.FromError(err); ok {
			if statusErr.Code() == codes.DeadlineExceeded {
				fmt.Println("Timeout was hit! Deadline was exceeded")
			} else {
				log.Fatalf("unexpected err: %v", statusErr)
			}
		} else {
			log.Fatalf("Error while calling MultiplyWithDeadline RPC: %v", err)
		}
		return
	}

	fmt.Printf("Response from MultiplyWithDeadlineRequest with timeout %v: %v\n", timeout, res.Result)
}
