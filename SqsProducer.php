<?php

declare(strict_types=1);

namespace Enqueue\Sqs;

use Interop\Queue\InvalidDestinationException;
use Interop\Queue\InvalidMessageException;
use Interop\Queue\PriorityNotSupportedException;
use Interop\Queue\PsrDestination;
use Interop\Queue\PsrMessage;
use Interop\Queue\PsrProducer;
use Interop\Queue\TimeToLiveNotSupportedException;

class SqsProducer implements PsrProducer
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
    public function send(PsrDestination $destination, PsrMessage $message): void
    {
        InvalidDestinationException::assertDestinationInstanceOf($destination, SqsDestination::class);
        InvalidMessageException::assertMessageInstanceOf($message, SqsMessage::class);

        $arguments = $this->arguments($destination, $message);

        $result = $this->context->getClient()->sendMessage($arguments);

        if (false == $result->hasKey('MessageId')) {
            throw new \RuntimeException('Message was not sent');
        }
    }

    /**
     * @return SqsProducer
     */
    public function setDeliveryDelay(int $deliveryDelay = null): PsrProducer
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
    public function setPriority(int $priority = null): PsrProducer
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
    public function setTimeToLive(int $timeToLive = null): PsrProducer
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
     * @param PsrDestination $destination
     * @param PsrMessage[] $messages
     */
    public function sendAll(PsrDestination $destination, array $messages): void
    {
        InvalidDestinationException::assertDestinationInstanceOf($destination, SqsDestination::class);

        $arguments = [];
        foreach ($messages as $message) {
            $arguments[] = $this->arguments($destination, $message);
        }

        $result = $this->context->getClient()->sendMessageBatch($arguments);

        if ($result->hasKey('Failed') && count($result->get('Failed')) > 0) {
            throw new \RuntimeException('Messages were not sent: ' . json_encode($result->get('Failed')));
        }
    }

    /**
     * @param SqsDestination $destination
     * @param SqsMessage     $message
     * @return array
     * @throws InvalidMessageException
     */
    private function arguments(PsrDestination $destination, PsrMessage $message): array
    {
        InvalidMessageException::assertMessageInstanceOf($message, SqsMessage::class);

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

        return $arguments;
    }
}
