import {Fetch} from "@/api/request";

const Urls = {
    getCurrentClipboard: "/api/clipboard",
    getConfig: "/api/settings",
    modifyConfig: "/api/settings",
}

class ClipboardContent {
    kind: string;
    text: string;
    timestamp?: number;
    from?: string;
    result: boolean;
    message?: string;

    constructor(kind: string,
                text: string,
                result: boolean,
                timestamp?: number,
                from?: string,
                message?: string,) {
        this.kind = kind;
        this.text = text;
        this.result = result;
        this.timestamp = timestamp;
        this.from = from;
        this.message = message;
    }

    formatTime(): string {
        const date = new Date(this.timestamp * 1000); // 将秒转换为毫秒
        // 转换为本地时间字符串
        return date.toLocaleString()
    }
}


function ReadClipboard(): ClipboardContent {
    const {data, error} = Fetch(Urls.getCurrentClipboard)
}

export default {
    Urls,
    ClipboardContent,
    ReadClipboard,
}