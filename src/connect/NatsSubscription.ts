import { INatsMessageListener } from "./INatsMessageListener";

export class NatsSubscription {
    public subject: string;
    public filter: boolean;
    public options: any;
    public handler: any;
    public listener: INatsMessageListener;
}