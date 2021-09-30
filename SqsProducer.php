<?php

declare(strict_types=1);

namespace Enqueue\Sqs;

use Interop\Queue\Destination;
use Interop\Queue\Exception\InvalidDestinationException;
use Interop\Queue\Exception\InvalidMessageException;
use Interop\Queue\Exception\PriorityNotSupportedException;
use Interop\Queue\Exception\TimeToLiveNotSupportedException;
use Interop\Queue\Message;
use Interop\Queue\Producer;

class SqsProducer implements Producer
{
    /**
     * @var int|null
     */
    private $deliveryDelay;

    /**
     * @var SqsContext
     */
    private $context;

    public function __construct(SqsContext $context)
    {
        $this->context = $context;
    }

    /**
     * @param SqsDestination $destination
     * @param SqsMessage     $message
     */
    public function send(Destination $destination, Message $message): void
    {
        InvalidDestinationException::assertDestinationInstanceOf($destination, SqsDestination::class);
        InvalidMessageException::assertMessageInstanceOf($message, SqsMessage::class);

        $arguments = $this->arguments($destination, $message);

        $arguments = [
            '@region' => $destination->getRegion(),
            'MessageAttributes' => [
                'Headers' => [
                    'DataType' => 'String',
                    'StringValue' => json_encode([$message->getHeaders(), $message->getProperties()]),
                ],
            ],
            'MessageBody' => $body,
            'QueueUrl' => $this->context->getQueueUrl($destination),
        ];

        if (null !== $this->deliveryDelay) {
            $arguments['DelaySeconds'] = (int) $this->deliveryDelay / 1000;
        }

        if ($message->getDelaySeconds()) {
            $arguments['DelaySeconds'] = $message->getDelaySeconds();
        }

        if ($message->getMessageDeduplicationId()) {
            $arguments['MessageDeduplicationId'] = $message->getMessageDeduplicationId();
        }

        if ($message->getMessageGroupId()) {
            $arguments['MessageGroupId'] = $message->getMessageGroupId();
        }

        $result = $this->context->getSqsClient()->sendMessage($arguments);

        if (false == $result->hasKey('MessageId')) {
            throw new \RuntimeException('Message was not sent');
        }
    }

    /**
     * @return SqsProducer
     */
    public function setDeliveryDelay(int $deliveryDelay = null): Producer
    {
        $this->deliveryDelay = $deliveryDelay;

        return $this;
    }

    public function getDeliveryDelay(): ?int
    {
        return $this->deliveryDelay;
    }

    /**
     * @return SqsProducer
     */
    public function setPriority(int $priority = null): Producer
    {
        if (null === $priority) {
            return $this;
        }

        throw PriorityNotSupportedException::providerDoestNotSupportIt();
    }

    public function getPriority(): ?int
    {
        return null;
    }

    /**
     * @return SqsProducer
     */
    public function setTimeToLive(int $timeToLive = null): Producer
    {
        if (null === $timeToLive) {
            return $this;
        }

        throw TimeToLiveNotSupportedException::providerDoestNotSupportIt();
    }

    public function getTimeToLive(): ?int
    {
        return null;
    }

    /**
     * @param Destination $destination
     * @param array $messages
     * @return array
     */
    public function sendAll(Destination $destination, array $messages): array
    {
        InvalidDestinationException::assertDestinationInstanceOf($destination, SqsDestination::class);

        $arguments = $this->arguments($destination, $messages);

        $result = $this->context->getClient()->sendMessageBatch($arguments);

        $successful = [];
        if ($result->hasKey('Successful') && count($result->get('Successful')) > 0) {
            $successful = array_map(function ($entry) {
                return $entry['Id'];
            }, $result->get('Successful'));
        }

        return $successful;
    }

    /**
     * @param SqsDestination           $destination
     * @param SqsMessage|SqsMessage[]  $messages
     * @return array
     * @throws InvalidMessageException
     */
    private function arguments(Destination $destination, $messages): array
    {
        $arguments = [
            'QueueUrl' => $this->context->getQueueUrl($destination),
        ];

        if (is_array($messages)) {
            $arguments['Entries'] = [];
            foreach ($messages as $message) {
                $arguments['Entries'][] = array_merge(
                    ['Id' => $message->getMessageId()],
                    $this->messageArguments($message)
                );
            }
        } else {
            $arguments = array_merge($arguments, $this->messageArguments($messages));
        }

        return $arguments;
    }

    /**
     * @param SqsMessage $message
     * @return array
     * @throws InvalidMessageException
     */
    private function messageArguments(SqsMessage $message): array
    {
        InvalidMessageException::assertMessageInstanceOf($message, SqsMessage::class);

        $body = $message->getBody();
        if (empty($body)) {
            throw new InvalidMessageException('The message body must be a non-empty string.');
        }

        $body = $message->getBody();
        if (empty($body)) {
            throw new InvalidMessageException('The message body must be a non-empty string.');
        }
        $arguments = [
            'MessageAttributes' => [
                'Headers' => [
                    'DataType' => 'String',
                    'StringValue' => json_encode([$message->getHeaders(), $message->getProperties()]),
                ],
            ],
            'MessageBody' => $body,
        ];
        if (null !== $this->deliveryDelay) {
            $arguments['DelaySeconds'] = (int) $this->deliveryDelay / 1000;
        }

        if ($message->getDelaySeconds()) {
            $arguments['DelaySeconds'] = $message->getDelaySeconds();
        }

        if ($message->getMessageDeduplicationId()) {
            $arguments['MessageDeduplicationId'] = $message->getMessageDeduplicationId();
        }

        if ($message->getMessageGroupId()) {
            $arguments['MessageGroupId'] = $message->getMessageGroupId();
        }

        return $arguments;
    }
}
