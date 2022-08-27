var express = require('express');
var router = express.Router();
const { ServiceBusClient } = require("@azure/service-bus");
const e = require('express');
// Define connection string and related Service Bus entity names here
const connectionString = process.env.SERVICEBUS_CONNECTION_STRING;
const queueName = process.env.QUEUE_NAME;
const topicName = process.env.TOPIC_NAME;
const topicSubscription1Name = process.env.TOPIC_SUBSCRIPTION_1_NAME;
const topicSubscription2Name = process.env.TOPIC_SUBSCRIPTION_2_NAME;

const firstSetOfMessages = [
    {
        body: {
            "timestamp": "2020-02-12T17:02:01.742Z",
            "id": "topic_one_id",
            "packageId": "root_map_one_id",
            "parentId": "root_map_one_id",
            "contentType": "topic",
            "title": "Topic one title V1",
            "navtitle": "Topic one navigation title V1",
            "topicType": "learningAssessment",
            "relatedLinks": [
                {
                    "_id": "link-id-1",
                    "_type": "reference",
                    "children": [
                        {
                            "_type": "span",
                            "marks": [],
                            "text": "First link"
                        }
                    ],
                    "xtrf": "xtrf"
                }
            ],
            "metadata": {
                "endUser": [
                    "learner"
                ],
                "isSelfAssessment": false
            },
            "class": {
                "local": "teacher-guide",
                "inherited": "product",
                "aggregated": [
                    "product"
                ]
            },
            "language": "",
            "particles": [
                "topic_one_id_particle_one_id"
            ],
            "ancestors": [
                "root_map_one_id"
            ]
        }
    },
    {
        body: {
            "timestamp": "2020-01-11T17:02:01.742Z",
            "id": "topic_two_id",
            "packageId": "root_map_one_id",
            "parentId": "root_map_one_id",
            "contentType": "topic",
            "title": "Topic one title V2",
            "navtitle": "Topic one navigation title V2",
            "topicType": "learningAssessment",
            "relatedLinks": [],
            "metadata": {
                "endUser": [
                    "learner"
                ],
                "isSelfAssessment": true
            },
            "class": {
                "local": "teacher-guide",
                "inherited": "product",
                "aggregated": [
                    "product"
                ]
            },
            "language": "",
            "particles": [
                "topic_one_id_particle_one_id"
            ],
            "ancestors": [
                "root_map_one_id"
            ]
        }
    },
    {
        body: {
            "timestamp": "2020-02-14T17:02:01.742Z",
            "packageId": "root_map_three_id",
            "parentId": "root_map_three_id",
            "contentType": "topic",
            "title": "Topic three title V1",
            "navtitle": "Topic three navigation title V1",
            "topicType": "learningAssessment",
            "relatedLinks": [
                {
                    "_id": "link-id-1",
                    "_type": "reference",
                    "children": [
                        {
                            "_type": "span",
                            "marks": [],
                            "text": "First link"
                        }
                    ],
                    "xtrf": "xtrf"
                }
            ],
            "metadata": {
                "endUser": [
                    "learner"
                ],
                "isSelfAssessment": false
            },
            "class": {
                "local": "teacher-guide",
                "inherited": "product",
                "aggregated": [
                    "product"
                ]
            },
            "language": "",
            "particles": [
                "topic_one_id_particle_one_id"
            ],
            "ancestors": [
                "root_map_one_id"
            ]
        }
    },
    {
        body: {
            "timestamp": "2020-02-09T17:02:01.742Z",
            "packageId": "root_map_three_id",
            "parentId": "root_map_three_id",
            "contentType": "topic",
            "title": "Topic one title V1",
            "navtitle": "Topic one navigation title V1",
            "topicType": "learningAssessment",
            "relatedLinks": [
                {
                    "_id": "link-id-1",
                    "_type": "reference",
                    "children": [
                        {
                            "_type": "span",
                            "marks": [],
                            "text": "First link"
                        }
                    ],
                    "xtrf": "xtrf"
                }
            ],
            "metadata": {
                "endUser": [
                    "learner"
                ],
                "isSelfAssessment": false
            },
            "class": {
                "local": "teacher-guide",
                "inherited": "product",
                "aggregated": [
                    "product"
                ]
            },
            "language": "",
            "particles": [
                "topic_one_id_particle_one_id"
            ],
            "ancestors": [
                "root_map_one_id"
            ]
        }
    },
];

/**
 * @swagger
 * components:
 *   schemas:
 *     Message:
 *       type: object
 *       required:
 *         - id
 *         - packageId
 *       properties:
 *         timestamp:
 *           type: date
 *           description: The message current timestamp
 *         id:
 *           type: string
 *           description: The id of the message
 *         packageId:
 *           type: string
 *           description: The package id
 *         contentType:
 *           type: string
 *           description: The content type
 *         title:
 *           type: string
 *           description: The title
 *         navtitle:
 *           type: string
 *           description: The nav Title
 *         topicType:
 *           type: string
 *           description: The topic type
 *         relatedLinks:
 *           type: array
 *           description: The related link array of links 
 *         metadata:
 *           type: object
 *           description: The metadata object of metadata properties
 *         language:
 *           type: string
 *           description: The language name
 *         particles:
 *           type: array
 *           description: The particles array
 *         ancestors:
 *           type: array
 *           description: The ancestors array
 *       example:
 *         timestamp: 2020-02-19T17:02:01.742Z
 *         id: topic_one_id
 *         packageId: root_map_one_id
 *         parentId: root_map_one_id
 *         contentType: topic
 *         title: Topic one title V2
 *         navtitle: Topic one navigation title V2
 *         topicType: learningAssessment
 *         relatedLinks: []
 *         metadata: {}
 *         class: {}
 *         language: English
 *         particles: []
 *         ancestors: []
 */

/**
 * @swagger
 * tags:
 *   name: Send & Receive Messages
 *   description: The Send and Receive Azure Bus Messages API's
 */


/**
* @swagger
* /bus/send-message:
*   post:
*     summary: Submit the message to Azure Bus Topic
*     tags: [Send & Receive Messages]
*     parameters:
*      - in: query
*        name: topicName
*        required: true
*        schema: 
*          type: string
*        description: Azure Service Bus Topic Name
*     requestBody:
*       required: true
*       content:
*           application/json:
*             schema:
*               $ref: '#/components/schemas/Message'   
*     responses:
*       200:
*         description: The list of the messages
*         content:
*           application/json:
*             schema:
*               type: array
*               $ref: '#/components/schemas/Message'
*/

// Send Message
router.post('/send-message', async function (req, res) {
    const sbClient = new ServiceBusClient(connectionString);
    // createSender() can also be used to create a sender for a topic.
    const sender = sbClient.createSender(req.query.topicName);

    try {
        // Tries to send all messages in a single batch.
        // Will fail if the messages cannot fit in a batch.
        console.log("body ", req.body)
        await sender.sendMessages({ body: req.body });

        // Close the sender
        console.log(`Done sending, closing...`);
        await sender.close();
        return res.json({ message: 'Message Sent Successfully', success: true });
    } finally {
        await sbClient.close();
    }
});

// Receive Queue Messages
router.get('/receive-queue-message', async function (req, res) {
    const sbClient = new ServiceBusClient(connectionString);
    // If receiving from a subscription you can use the createReceiver(topicName, subscriptionName) overload
    // instead.
    const queueReceiver = sbClient.createReceiver(queueName);
    try {
        let allMessages = [], formattedMessages = [];

        console.log(`Receiving 10 messages...`);

        while (allMessages.length < 10) {
            // NOTE: asking for 10 messages does not guarantee that we will return
            // all 10 at once so we must loop until we get all the messages we expected.
            const messages = await queueReceiver.receiveMessages(10, {
                maxWaitTimeInMs: 5 * 1000,
            });

            if (!messages.length) {
                console.log("No more messages to receive");
                break;
            }

            console.log(`Received ${messages.length} messages`);
            allMessages.push(...messages);
            console.log("L ", allMessages.length);
            for (let message of messages) {
                console.log(`  Message: '${message.body}'`);
                formattedMessages.push({ body: message.body })
                // completing the message will remove it from the remote queue or subscription.
                await queueReceiver.completeMessage(message);
            }
        }

        await queueReceiver.close();
        return res.json({ message: 'Message Received Successfully', success: true, data: formattedMessages });

    } finally {
        await sbClient.close();
    }
});

/**
 * @swagger
 * /bus/receive-topic-message:
 *   get:
 *     summary: Returns a list of all the active topic [subscription] messages and deletes them afterwards
 *     tags: [Send & Receive Messages]
 *     parameters:
 *      - in: query
 *        name: topicName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Topic Name
 *      - in: query
 *        name: subscriptionName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Subscrioption Name  
 *     responses:
 *       200:
 *         description: The list of the messages
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               $ref: '#/components/schemas/Message'
 */

// Receive Topic Messages
router.get('/receive-topic-message', async function (req, res) {
    const sbClient = new ServiceBusClient(connectionString);
    // If receiving from a subscription you can use the createReceiver(topicName, subscriptionName) overload
    // instead.
    const receiver = sbClient.createReceiver(req.query.topicName, req.query.subscriptionName);
    let formattedMessages = [];
    try {
        const subscription = receiver.subscribe({

            processMessage: async (brokeredMessage) => {
                console.log(`Received message: ${brokeredMessage.body}`);
                formattedMessages.push({ body: brokeredMessage.body })
            },
            // This callback will be called for any error that occurs when either in the receiver when receiving the message
            // or when executing your `processMessage` callback or when the receiver automatically completes or abandons the message.
            processError: async (args) => {
                console.log(`Error from source ${args.errorSource} occurred: `, args.error);

                // the `subscribe() call will not stop trying to receive messages without explicit intervention from you.
                if (isServiceBusError(args.error)) {
                    switch (args.error.code) {
                        case "MessagingEntityDisabled":
                        case "MessagingEntityNotFound":
                        case "UnauthorizedAccess":
                            // It's possible you have a temporary infrastructure change (for instance, the entity being
                            // temporarily disabled). The handler will continue to retry if `close()` is not called on the subscription - it is completely up to you
                            // what is considered fatal for your program.
                            console.log(
                                `An unrecoverable error occurred. Stopping processing. ${args.error.code}`,
                                args.error
                            );
                            await subscription.close();
                            break;
                        case "MessageLockLost":
                            console.log(`Message lock lost for message`, args.error);
                            break;
                        case "ServiceBusy":
                            // choosing an arbitrary amount of time to wait.
                            await delay(1000);
                            break;
                    }
                }
            },
        });

        // Waiting long enough before closing the receiver to receive messages
        console.log(`Receiving messages for 20 seconds before exiting...`);
        await sleep(20000);

        console.log(`Closing...`);
        await receiver.close();
        return res.json({ message: 'Message Received Successfully', messageCount: formattedMessages.length, success: true, data: formattedMessages });

    } finally {
        await sbClient.close();
    }
});

/**
 * @swagger
 * tags:
 *   name: Peek Messages
 *   description: Peek Active and DLQ Messages API's
 */

/**
 * @swagger
 * /bus/peek-topic-message:
 *   get:
 *     summary: Peek all the active messages in the topic [subscription]
 *     tags: [Peek Messages]
 *     parameters:
 *      - in: query
 *        name: topicName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Topic Name
 *      - in: query
 *        name: subscriptionName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Subscrioption Name  
 *     responses:
 *       200:
 *         description: The list of the messages
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               $ref: '#/components/schemas/Message'
 */

// Peek Topic Messages
router.get('/peek-topic-message', async function (req, res) {
    const sbClient = new ServiceBusClient(connectionString);
    // If receiving from a subscription you can use the createReceiver(topicName, subscriptionName) overload
    // instead.
    const receiver = sbClient.createReceiver(req.query.topicName, req.query.subscriptionName);
    let formattedMessages = [];
    try {
        // peeking messages does not lock or remove messages from a queue or subscription.
        // For locking and/or removal, look at the `receiveMessagesLoop` or `receiveMessagesStreaming` samples,
        // which cover using a receiver with a `receiveMode`.
        console.log(`Attempting to peek 1000 messages at a time`);
        const peekedMessages = await receiver.peekMessages(1000);

        console.log(`Got ${peekedMessages.length} messages.`);

        for (let i = 0; i < peekedMessages.length; ++i) {
            console.log(`Peeked message #${i}: ${peekedMessages[i].body}`);
            formattedMessages.push({ body: peekedMessages[i].body })
        }

        await receiver.close();
        return res.json({ message: 'Message Peeked Successfully', messageCount: formattedMessages.length, success: true, data: formattedMessages });

    } finally {
        await sbClient.close();
    }
});

/**
 * @swagger
 * /bus/peek-topic-dlq-message:
 *   get:
 *     summary: Peek all the DLQ messages in the topic [subscription]
 *     tags: [Peek Messages]
 *     parameters:
 *      - in: query
 *        name: topicName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Topic Name
 *      - in: query
 *        name: subscriptionName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Subscrioption Name  
 *     responses:
 *       200:
 *         description: The list of the messages
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               $ref: '#/components/schemas/Message'
 */

// Peek Topic DLQ Messages
router.get('/peek-topic-dlq-message', async function (req, res) {
    const sbClient = new ServiceBusClient(connectionString);
    // If receiving from a subscription you can use the createReceiver(topicName, subscriptionName) overload
    // instead.
    const receiver = sbClient.createReceiver(req.query.topicName, req.query.subscriptionName, { subQueueType: "deadLetter" });
    let formattedMessages = [];
    try {
        // peeking messages does not lock or remove messages from a queue or subscription.
        // For locking and/or removal, look at the `receiveMessagesLoop` or `receiveMessagesStreaming` samples,
        // which cover using a receiver with a `receiveMode`.
        console.log(`Attempting to peek 1000 messages at a time`);
        const peekedMessages = await receiver.peekMessages(1000);

        console.log(`Got ${peekedMessages.length} messages.`);

        for (let i = 0; i < peekedMessages.length; ++i) {
            console.log(`Peeked message #${i}: ${peekedMessages[i].body}`);
            formattedMessages.push({ body: peekedMessages[i].body })
        }

        await receiver.close();
        return res.json({ message: 'DLQ Messages Peeked Successfully', messageCount: formattedMessages.length, success: true, data: formattedMessages });

    } finally {
        await sbClient.close();
    }
});

/**
 * @swagger
 * tags:
 *   name: Delete Messages
 *   description: Delete Active and DLQ Message API's
 */

/**
 * @swagger
 * /bus/delete-active-topic-message:
 *   get:
 *     summary: Delete all the active messages in the topic [subscription]
 *     tags: [Delete Messages]
 *     parameters:
 *      - in: query
 *        name: topicName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Topic Name
 *      - in: query
 *        name: subscriptionName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Subscrioption Name  
 *     responses:
 *       200:
 *         description: The list of the messages
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               $ref: '#/components/schemas/Message'
 */

// Delete Active Topic Messages
router.get('/delete-active-topic-message', async function (req, res) {
    const sbClient = new ServiceBusClient(connectionString);
    // If receiving from a subscription you can use the createReceiver(topicName, subscriptionName) overload
    // instead.
    const receiver = sbClient.createReceiver(req.query.topicName, req.query.subscriptionName);
    try {
        // run the loop until all the messages are deleted
        while (true) {
            let receivedMessage = await receiver.receiveMessages(10, { maxWaitTimeInMs: 5 * 1000 });
            if (!receivedMessage.length) {
                console.log("No more messages to retrive");
                break;
            }

            console.log(`Received ${receivedMessage.length} messages`);
            for (let message of receivedMessage) {
                console.log(`  Message: '${message.body}'`);
                // completing the message will remove it from the remote queue or subscription.
                await receiver.completeMessage(message);
            }
        }
        await receiver.close();
        return res.json({ message: 'Message Deleted Successfully', success: true });

    } finally {
        await sbClient.close();
    }
});

/**
 * @swagger
 * /bus/delete-dlq-topic-message:
 *   get:
 *     summary: Delete all the DLQ messages in the topic [subscription]
 *     tags: [Delete Messages]
 *     parameters:
 *      - in: query
 *        name: topicName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Topic Name
 *      - in: query
 *        name: subscriptionName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Subscrioption Name  
 *     responses:
 *       200:
 *         description: The list of the messages
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               $ref: '#/components/schemas/Message'
 */

// Delete DLQ Topic Messages
router.get('/delete-dlq-topic-message', async function (req, res) {
    const sbClient = new ServiceBusClient(connectionString);
    // If receiving from a subscription you can use the createReceiver(topicName, subscriptionName) overload
    // instead.
    const receiver = sbClient.createReceiver(req.query.topicName, req.query.subscriptionName, { subQueueType: "deadLetter" });
    try {
        // run the loop until all the messages are deleted
        while (true) {
            let receivedMessage = await receiver.receiveMessages(10, { maxWaitTimeInMs: 5 * 1000 });
            if (!receivedMessage.length) {
                console.log("No more messages to retrive");
                break;
            }

            console.log(`Received ${receivedMessage.length} messages`);
            for (let message of receivedMessage) {
                console.log(`  Message: '${message.body}'`);
                // completing the message will remove it from the remote queue or subscription.
                await receiver.completeMessage(message);
            }
        }
        await receiver.close();
        return res.json({ message: 'Message Deleted Successfully', success: true });

    } finally {
        await sbClient.close();
    }
});

/**
 * @swagger
 * tags:
 *   name: Process DLQs
 *   description: Move Inaccurate Messages to DLQ and Republish DLQ Messages API's
 */

/**
 * @swagger
 * /bus/process-message-for-dlq:
 *   get:
 *     summary: Process all the messages in the topic [subscription] and move invalid them to DLQ
 *     tags: [Process DLQs]
 *     parameters:
 *      - in: query
 *        name: topicName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Topic Name
 *      - in: query
 *        name: subscriptionName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Subscrioption Name  
 *     responses:
 *       200:
 *         description: The list of the messages
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               $ref: '#/components/schemas/Message'
 */

// Process Messages To DLQ
router.get('/process-message-for-dlq', async function (req, res) {
    const sbClient = new ServiceBusClient(connectionString);
    // If receiving from a subscription you can use the createReceiver(topicName, subscriptionName) overload
    // instead.
    const receiver = sbClient.createReceiver(req.query.topicName, req.query.subscriptionName);
    try {
        // run the loop until all the messages are deleted
        while (true) {
            let receivedMessage = await receiver.receiveMessages(10, { maxWaitTimeInMs: 5 * 1000 });
            if (!receivedMessage.length) {
                console.log("No more messages to retrive");
                break;
            }

            console.log(`Received ${receivedMessage.length} messages`);
            for (let message of receivedMessage) {
                console.log(`  Message: '${message.body}'`);
                if (message.body.hasOwnProperty('id')) {
                    console.log("valid message")
                    // await receiver.completeMessage(message);
                } else {
                    await receiver.deadLetterMessage(message, {
                        deadLetterReason: "Incorrect Object",
                        deadLetterErrorDescription: "Object Missing Field id",
                    });
                }
                // completing the message will remove it from the remote queue or subscription.
            }
        }
        await receiver.close();
        return res.json({ message: 'Message Processed Successfully', success: true });

    } finally {
        await sbClient.close();
    }
});

/**
 * @swagger
 * /bus/republish-dlq-topic-message:
 *   get:
 *     summary: Fetch all the DLQ messages from topic [subscription] fix them and Republish them
 *     tags: [Process DLQs]
 *     parameters:
 *      - in: query
 *        name: topicName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Topic Name
 *      - in: query
 *        name: subscriptionName
 *        required: true
 *        schema: 
 *          type: string
 *        description: Azure Service Bus Subscrioption Name  
 *     responses:
 *       200:
 *         description: The list of the messages
 *         content:
 *           application/json:
 *             schema:
 *               type: array
 *               $ref: '#/components/schemas/Message'
 */

// Republish DLQ Topic Messages
router.get('/republish-dlq-topic-message', async function (req, res) {
    const sbClient = new ServiceBusClient(connectionString);
    // If receiving from a subscription you can use the createReceiver(topicName, subscriptionName) overload
    // instead.
    const receiver = sbClient.createReceiver(req.query.topicName, req.query.subscriptionName, { subQueueType: "deadLetter" });
    try {
        // run the loop until all the all the DLQ messages are republished
        while (true) {
            let receivedMessage = await receiver.receiveMessages(10, { maxWaitTimeInMs: 5 * 1000 });
            if (!receivedMessage.length) {
                console.log("No more messages to retrive");
                break;
            }

            console.log(`Received ${receivedMessage.length} messages`);
            for (let message of receivedMessage) {
                console.log(`  Message: '${message.body}'`);
                // completing the message will remove it from the remote queue or subscription.
                await fixAndResendMessage(message, topicName, topicSubscription2Name)
                await receiver.completeMessage(message);
            }
        }
        await receiver.close();
        return res.json({ message: 'Message Republished Successfully', success: true });

    } finally {
        await sbClient.close();
    }
});

async function fixAndResendMessage(oldMessage, topic, subscription) {
    const sbClient = new ServiceBusClient(connectionString);
    // createSender() can also be used to create a sender for a topic.
    const sender = sbClient.createSender(topic, subscription);

    // Inspect given message and make any changes if necessary
    oldMessage.body.id = 'topic_three_message'

    console.log(">>>>> Cloning the message from DLQ and resending it - ", oldMessage.body);

    await sender.sendMessages(oldMessage);
    await sender.close();
}

function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

module.exports = router;