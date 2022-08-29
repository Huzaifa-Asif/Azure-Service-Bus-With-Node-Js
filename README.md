# Azure Service Buss 

Azure Service Bus is a fully managed enterprise message broker with message queues and publish-subscribe topics (in a namespace). Service Bus is used to decouple applications and services from each other, providing the following benefits:

Data is transferred between different applications and services using messages. Some common messaging scenarios are:
- Messaging: Transfer business data, such as sales or purchase orders, journals, or inventory movements.
- Decouple applications: Improve reliability and scalability of applications and services. Producer and consumer don't have to be online or readily available at the same time. The load is leveled such that traffic spikes don't overtax a service.
- Load balancing: Allow for multiple competing consumers to read from a queue at the same time, each safely obtaining exclusive ownership to specific messages.
- Topics and subscriptions: Enable 1:n relationships between publishers and subscribers, allowing subscribers to select particular messages from a published message stream.
- Transactions:  Allows you to do several operations, all in the scope of an atomic transaction, For example, the following operations can be done in the scope of a transaction.
1 -- Obtain a message from one queue.
2 -- Post results of processing to one or more different queues.
3 -- Move the input message from the original queue.

# How to run locally

### Install the packages:

First run npm install to install the packages

`npm install`

### Create a `.env` file into the root folder:

You will need the following Environment Variables and Values to the `.env` file:

```
AZURE_SERVICE_BUS_CONNECTION_STRING=
QUEUE_NAME=InfinitasQueue
TOPIC_NAME=infinitastopic
TOPIC_SUBSCRIPTION_1_NAME=sub1
TOPIC_SUBSCRIPTION_2_NAME=sub2
```

### Connecting to the Service Bus

You will need a connection String value for the `AZURE_SERVICE_BUS_CONNECTION_STRING` you can grab this by following these instructions: 

1. Goto [portal.azure.com](portal.azure.com)
2. Login into your account
3. Search for `service-bus`
4. Create a buss service `test-bus`
5. Select `Shared Access Policies` in the settings section of the sidebar menu
6. Open `RootManageSharedAccessKey` policy 
7. Copy `Primary Connection String` or `Secondary Connection String`
8. Paste the value as `AZURE_SERVICE_BUS_CONNECTION_STRING` in your `.env` file

### Run the app:
We can now run the app in Local

#### Local 
`npm run start`
