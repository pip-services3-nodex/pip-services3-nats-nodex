import { INatsMessageListener } from "./INatsMessageListener";
export declare class NatsSubscription {
    subject: string;
    filter: boolean;
    options: any;
    handler: any;
    listener: INatsMessageListener;
}
