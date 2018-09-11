<?php

namespace Enqueue\Sqs\Tests;

use Aws\Result;
use Aws\Sqs\SqsClient;
use Enqueue\Sqs\SqsContext;
use Enqueue\Sqs\SqsDestination;
use Enqueue\Sqs\SqsMessage;
use Enqueue\Sqs\SqsProducer;
use Enqueue\Test\ClassExtensionTrait;
use Interop\Queue\InvalidDestinationException;
use Interop\Queue\InvalidMessageException;
use Interop\Queue\PsrDestination;
use Interop\Queue\PsrProducer;

class SqsProducerTest extends \PHPUnit_Framework_TestCase
{
    use ClassExtensionTrait;

    public function testShouldImplementProducerInterface()
    {
        $this->assertClassImplements(PsrProducer::class, SqsProducer::class);
    }

    public function testCouldBeConstructedWithRequiredArguments()
    {
        new SqsProducer($this->createSqsContextMock());
    }

    public function testShouldThrowIfBodyOfInvalidType()
    {
        $this->expectException(InvalidMessageException::class);
        $this->expectExceptionMessage('The message body must be a non-empty string.');

        $producer = new SqsProducer($this->createSqsContextMock());

        $message = new SqsMessage('');

        $producer->send(new SqsDestination(''), $message);
    }

    public function testShouldThrowIfDestinationOfInvalidType()
    {
        $this->expectException(InvalidDestinationException::class);
        $this->expectExceptionMessage('The destination must be an instance of Enqueue\Sqs\SqsDestination but got Mock_PsrDestinat');

        $producer = new SqsProducer($this->createSqsContextMock());

        $producer->send($this->createMock(PsrDestination::class), new SqsMessage());
    }

    public function testShouldThrowIfSendMessageFailed()
    {
        $client = $this->createSqsClientMock();
        $client
            ->expects($this->once())
            ->method('sendMessage')
            ->willReturn(new Result())
        ;

        $context = $this->createSqsContextMock();
        $context
            ->expects($this->once())
            ->method('getQueueUrl')
            ->willReturn('theQueueUrl')
        ;
        $context
            ->expects($this->once())
            ->method('getClient')
            ->will($this->returnValue($client))
        ;

        $destination = new SqsDestination('queue-name');
        $message = new SqsMessage('foo');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Message was not sent');

        $producer = new SqsProducer($context);
        $producer->send($destination, $message);
    }

    public function testShouldSendMessage()
    {
        $expectedArguments = [
            'MessageAttributes' => [
                'Headers' => [
                    'DataType' => 'String',
                    'StringValue' => '[{"hkey":"hvaleu"},{"key":"value"}]',
                ],
            ],
            'MessageBody' => 'theBody',
            'QueueUrl' => 'theQueueUrl',
            'DelaySeconds' => 12345,
            'MessageDeduplicationId' => 'theDeduplicationId',
            'MessageGroupId' => 'groupId',
        ];

        $client = $this->createSqsClientMock();
        $client
            ->expects($this->once())
            ->method('sendMessage')
            ->with($this->identicalTo($expectedArguments))
            ->willReturn(new Result())
        ;

        $context = $this->createSqsContextMock();
        $context
            ->expects($this->once())
            ->method('getQueueUrl')
            ->willReturn('theQueueUrl')
        ;
        $context
            ->expects($this->once())
            ->method('getClient')
            ->will($this->returnValue($client))
        ;

        $destination = new SqsDestination('queue-name');
        $message = new SqsMessage('theBody', ['key' => 'value'], ['hkey' => 'hvaleu']);
        $message->setDelaySeconds(12345);
        $message->setMessageDeduplicationId('theDeduplicationId');
        $message->setMessageGroupId('groupId');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Message was not sent');

        $producer = new SqsProducer($context);
        $producer->send($destination, $message);
    }

    public function testShouldSendDelayedMessage()
    {
        $expectedArguments = [
            'MessageAttributes' => [
                'Headers' => [
                    'DataType' => 'String',
                    'StringValue' => '[{"hkey":"hvaleu"},{"key":"value"}]',
                ],
            ],
            'MessageBody' => 'theBody',
            'QueueUrl' => 'theQueueUrl',
            'DelaySeconds' => 12345,
            'MessageDeduplicationId' => 'theDeduplicationId',
            'MessageGroupId' => 'groupId',
        ];

        $client = $this->createSqsClientMock();
        $client
            ->expects($this->once())
            ->method('sendMessage')
            ->with($this->identicalTo($expectedArguments))
            ->willReturn(new Result())
        ;

        $context = $this->createSqsContextMock();
        $context
            ->expects($this->once())
            ->method('getQueueUrl')
            ->willReturn('theQueueUrl')
        ;
        $context
            ->expects($this->once())
            ->method('getClient')
            ->will($this->returnValue($client))
        ;

        $destination = new SqsDestination('queue-name');
        $message = new SqsMessage('theBody', ['key' => 'value'], ['hkey' => 'hvaleu']);
        $message->setDelaySeconds(12345);
        $message->setMessageDeduplicationId('theDeduplicationId');
        $message->setMessageGroupId('groupId');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Message was not sent');

        $producer = new SqsProducer($context);
        $producer->setDeliveryDelay(5000);
        $producer->send($destination, $message);
    }

    /**
     * @return \PHPUnit_Framework_MockObject_MockObject|SqsContext
     */
    private function createSqsContextMock()
    {
        return $this->createMock(SqsContext::class);
    }

    /**
     * @return \PHPUnit_Framework_MockObject_MockObject|SqsClient
     */
    private function createSqsClientMock()
    {
        return $this
            ->getMockBuilder(SqsClient::class)
            ->disableOriginalConstructor()
            ->setMethods(['sendMessage', 'sendMessageBatch'])
            ->getMock()
        ;
    }

    public function testShouldSendMessageBatch()
    {
        $expectedArguments = [
            [
                'MessageAttributes' => [
                    'Headers' => [
                        'DataType' => 'String',
                        'StringValue' => '[{"hkey1":"hvaleu1"},{"key1":"value1"}]',
                    ],
                ],
                'MessageBody' => 'theBody1',
                'QueueUrl' => 'theQueueUrl',
                'DelaySeconds' => 12345,
                'MessageDeduplicationId' => 'theDeduplicationId1',
                'MessageGroupId' => 'groupId',
            ],
            [
                'MessageAttributes' => [
                    'Headers' => [
                        'DataType' => 'String',
                        'StringValue' => '[{"hkey2":"hvaleu2"},{"key2":"value2"}]',
                    ],
                ],
                'MessageBody' => 'theBody2',
                'QueueUrl' => 'theQueueUrl',
                'DelaySeconds' => 54321,
                'MessageDeduplicationId' => 'theDeduplicationId2',
                'MessageGroupId' => 'groupId2',
            ],
        ];

        $client = $this->createSqsClientMock();
        $client
            ->expects($this->once())
            ->method('sendMessageBatch')
            ->with($this->identicalTo($expectedArguments))
            ->willReturn(new Result([
                'Failed' => [
                    [
                        'Code' => 'code',
                        'Id' => 'id',
                        'Message' => 'MESSAGE',
                        'SenderFault' => true ,
                    ]
                ],
                'Successful' => [
                    [
                        'Id' => 'id',
                        'MD5OfMessageAttributes' => 'md5_of_message_attributes',
                        'MD5OfMessageBody' => 'md5_of_message_body',
                        'MessageId' => 'message_id',
                        'SequenceNumber' => 'sequence_number',
                    ],
                ],
            ]))
        ;

        $context = $this->createSqsContextMock();
        $context
            ->expects($this->exactly(2))
            ->method('getQueueUrl')
            ->willReturn('theQueueUrl')
        ;
        $context
            ->expects($this->once())
            ->method('getClient')
            ->will($this->returnValue($client))
        ;

        $destination = new SqsDestination('queue-name');

        $message1 = new SqsMessage('theBody1', ['key1' => 'value1'], ['hkey1' => 'hvaleu1']);
        $message1->setDelaySeconds(12345);
        $message1->setMessageDeduplicationId('theDeduplicationId1');
        $message1->setMessageGroupId('groupId');

        $message2 = new SqsMessage('theBody2', ['key2' => 'value2'], ['hkey2' => 'hvaleu2']);
        $message2->setDelaySeconds(54321);
        $message2->setMessageDeduplicationId('theDeduplicationId2');
        $message2->setMessageGroupId('groupId2');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('Messages were not sent: [{"Code":"code","Id":"id","Message":"MESSAGE","SenderFault":true}]');

        $producer = new SqsProducer($context);
        $producer->sendAll($destination, [$message1, $message2]);
    }
}
