#!/usr/bin/node
import FileSystem from 'node:fs/promises';
import ReadLine from 'node:readline';
import Path from 'node:path/posix';
import getopts from 'getopts';

const ARIB_INFINITY = 35999990;
type IAssDialogueAttr = Record<string,string> & {
    Layer?: string,
    Start: number,
    End: number,
    Style?: string,
    Name?: string,
    MarginL?: string,
    MarginR?: string,
    MarginV?: string,
    Effect?: string,
    Text: string
};
class AssDialogue {
    static PATTERN_TEXT_OVERRIDE = "\\{\\\\.*?\\}";
    attribs: IAssDialogueAttr;
    format: string[];

    constructor(str: string, format: string[]) {
        let idx = 0, pos = 'Dialogue:'.length;
        const attribs: Record<string,string> = {};
        while(1) {
            const prev = pos+1;
            if(idx == format.length-1) {
                pos = -1
            }
            else {
                pos = str.indexOf(',', prev);
            }
            const value = str.substring(prev, pos===-1?Infinity:pos);
            attribs[format[idx++]] = value;
            if(pos===-1) break;
        }
        this.attribs = {
            ...attribs,
            Start: AssDialogue.parseTSString(attribs.Start),
            End: AssDialogue.parseTSString(attribs.End),
        } as IAssDialogueAttr;
        this.format = format;
    }

    offset(ms: number) {
        return this.offsetStart(ms).offsetEnd(ms);
    }

    offsetStart(ms: number) {
        this.attribs.Start += ms;
        return this;
    }

    offsetEnd(ms: number) {
        if(this.attribs.End == ARIB_INFINITY) return this;
        this.attribs.End += ms;
        return this;
    }

    toString() {
        return `Dialogue: ${this.attribs.Layer},${AssDialogue.toASSTSString(this.attribs.Start)},${AssDialogue.toASSTSString(this.attribs.End)},${this.attribs.Style},${this.attribs.Name},${this.attribs.MarginL},${this.attribs.MarginR},${this.attribs.MarginV},${this.attribs.Effect},${this.attribs.Text.replace(/\{\\3a&Hff&\}/g,'')}`;
    }

    static from(str: string, format: string[]) {
        return new this(str, format);
    }

    static parseTSString(str: string) {
        const negative = str[0] == '-';
        const [time, ms] = str.substring(negative?1:0).split('.',2);
        const [hours, minutes, seconds] = time.split(':', 3);
        return (negative?-1:1)*(Number(hours)*60*60*1000 + Number(minutes)*60*1000 + Number(seconds)*1000 + Number(ms.substring(0, 3).padEnd(3,'0')));
    }

    static toTSString(time: number) {
        return this.toASSTSString(time);
    }

    static toASSTSString(time: number) {
        const sign = time < 0 ? '-' : '';
        const abstime = sign?-time:time;
        return `${sign}${(~~(abstime/(60*60*1000))).toString().padStart(1,'0')}:${(~~(abstime%(60*60*1000)/(60*1000))).toString().padStart(2,'0')}:${(~~(abstime%(60*1000)/1000)).toString().padStart(2,'0')}.${(~~(abstime%1000)).toString().substring(0,2).padEnd(2,'0')}`;
    }

    static toVTTTSString(time: number) {
        return `${(~~(time/(60*60*1000))).toString().padStart(2,'0')}:${(~~(time%(60*60*1000)/(60*1000))).toString().padStart(2,'0')}:${(~~(time%(60*1000)/1000)).toString().padStart(2,'0')}.${(~~(time%1000)).toString().substring(0,3).padEnd(3,'0')}`;
    }
}

const opts = getopts(process.argv.slice(2), {
    default: {
        hls_time: 2,
        hls_list_size: 5,
        hls_delete_threshold: 1,
        hls_ass_init_filename: 'init.ass',
    },
});

if(!opts._[0]) {
    console.error('[aribhls]', 'Output path required.');
    process.exit(-1);
}

const pwd = Path.dirname(opts._[0]);
await FileSystem.mkdir(pwd, {
    recursive: true,
});

const rl = ReadLine.createInterface({
    input: process.stdin,
    terminal: false,
    crlfDelay: Infinity,
});
const sighandler: NodeJS.SignalsListener = sig => {
    console.log(`Exiting normally, received signal ${sig}`)
    process.exit(0);
}
process.on('SIGINT', sighandler);
process.on('SIGTERM', sighandler);

const eventTemplate: string[] = [];
const buf: string[] = [];
const events: AssDialogue[] = [];
const playlist: {name: string, date: Date, duration: number, discontinuity: boolean}[] = [];
const playlist_name = Path.basename(opts._[0]);
let seq = 0;

for await(const line of rl) {
    if(line.startsWith('[Events]')) {
        eventTemplate.push(line);
        break;
    }
    buf.push(line);
}

const timebase = Date.now();
{
    const fp = await FileSystem.open(`${Path.resolve(pwd, opts.hls_ass_init_filename)}.tmp`, 'w');
    const stream = fp.createWriteStream();
    let line;
    while((line = buf.shift()) !== undefined) {
        stream.write(line);
        stream.write("\n");
    }
    await fp.close();
    await FileSystem.rename(`${Path.resolve(pwd, opts.hls_ass_init_filename)}.tmp`, `${Path.resolve(pwd, opts.hls_ass_init_filename)}`);
}

const eventFormat = await (async ():Promise<string[]>=>{
    for await(const line of rl) {
        if(line.startsWith('Format: ')) {
            const format = line.substring(line.indexOf(': ')+2).split(',').map(x=>x.replace(/^\s+|\s+$/g,''));
            eventTemplate.push(line);
            return format;
        }
        throw new TypeError('Invalid format');
    }
    return [];
})();

/* Read current playlist */ try {
    const fp = await FileSystem.open(`${Path.resolve(pwd, playlist_name)}`, 'r');
    console.log('[aribhls]', `Opening '${Path.resolve(pwd, playlist_name)}' for reading`);
    const rl = ReadLine.createInterface({
        input: fp.createReadStream(),
        terminal: false,
        crlfDelay: Infinity,
    });

    let duration, program_date_time, discontinuity;
    duration=program_date_time=discontinuity=undefined;
    for await(const line of rl) {
        if(line.startsWith('#')) {
            const tagName = line.substring(0, line.indexOf(':')===-1 ? undefined : line.indexOf(':'))
            switch(tagName) {
                case '#EXT-X-MEDIA-SEQUENCE':
                    seq = Number(line.substring(tagName.length+1, line.length-1));
                    break;
                case '#EXTM3U':
                case '#EXT-X-VERSION':
                case '#EXT-X-MEDIA':
                case '#EXT-X-TARGETDURATION':
                    break;
                case '#EXT-X-DISCONTINUITY': {
                    discontinuity = true;
                } break;
                case '#EXTINF': {
                    const parts = line.substring('#EXTINF:'.length, line.length-1).split('.',2);
                    duration = Number(parts[1].substring(0,3).padEnd(3, '0')) + (Number(parts[0])*1000);
                } break;
                case '#EXT-X-PROGRAM-DATE-TIME': {
                    program_date_time = new Date(line.substring('#EXT-X-PROGRAM-DATE-TIME:'.length));
                } break;
                default:
                    console.log(`Ignore unknown message '${line}'`);
                    break;
            }
            continue;
        }
        else {
            if(!program_date_time) {
                console.error('EXT-X-PROGRAM-DATE-TIME is not defined');
                continue;
            }
            if(!duration) {
                console.error('EXTINF is not defined');
                continue;
            }
            playlist.push({
                name: line,
                date: program_date_time,
                duration,
                discontinuity: !!discontinuity,
            })
            duration=program_date_time=discontinuity=undefined;
        }
    }
    rl.close();
    await fp.close();
} catch(ex) {
    if((ex as any).code != 'ENOENT') throw ex;
}

const write_playlist = async () => {
    const buf = [
        '#EXTM3U',
        '#EXT-X-VERSION:3',
        '#EXT-X-TARGETDURATION:5',
        `#EXT-X-MEDIA-SEQUENCE:${seq}`,
    ];
    {
        for(const segment of playlist) {
            const duration = segment.duration.toString();
            if(segment.discontinuity) {
                buf.push('#EXT-X-DISCONTINUITY');
            }
            buf.push(
                `#EXTINF:${duration.substring(0, duration.length - 3)}.${duration.substring(duration.length - 3)}000,`,
                `#EXT-X-PROGRAM-DATE-TIME:${segment.date.toISOString().replace(/Z$/,'+0000')}`,
                segment.name,
            )
        }
    }
    const fp = await FileSystem.open(`${Path.resolve(pwd, playlist_name)}.tmp`, 'w');
    console.log('[aribhls]', `Opening '${Path.resolve(pwd, playlist_name)}.tmp' for writing`);
    const stream = fp.createWriteStream();
    let line;
    while((line = buf.shift()) !== undefined) {
        stream.write(line);
        stream.write("\n");
    }
    stream.close();
    await fp.close();
    await FileSystem.rename(`${Path.resolve(pwd, playlist_name)}.tmp`, `${Path.resolve(pwd, playlist_name)}`);
};

let program_date_time = timebase;
let write_segment_to: NodeJS.Timeout;
let is_first = true, master_pl_written = false;
const write_segment = async ()=>{
    try {
        clearTimeout(write_segment_to);

        const now = new Date;
        const duration = now.getTime() - program_date_time;

        const name = `${now.toISOString().replace(/\..*$/,'').replace(/[-:]/g,'').replace('T','-')}.ass`;
        const fp = await FileSystem.open(`${Path.resolve(pwd, name)}.tmp`, 'w');
        console.log('[aribhls]', `Opening '${Path.resolve(pwd, name)}.tmp' for writing`);
        const stream = fp.createWriteStream();
        for(const line of eventTemplate) {
            stream.write(line);
            stream.write("\n");
        }
        let event;
        while((event = events.shift()) !== undefined) {
            stream.write(event.offset(timebase - program_date_time).toString());
            stream.write("\n");
        }

        stream.close();
        await fp.close();
        await FileSystem.rename(`${Path.resolve(pwd, name)}.tmp`, `${Path.resolve(pwd, name)}`);

        playlist.push({
            name,
            date: now,
            duration,
            discontinuity: is_first,
        });
        program_date_time = now.getTime();
        const rmq: string[] = [];
        for(let i=0;i<playlist.length-opts.hls_list_size;i++) {
            const row = playlist.shift();
            if(row) {
                seq++;
                rmq.push(`${Path.resolve(pwd, row.name)}`);
            }
        }
        await write_playlist();
        if(opts.master_pl_name) {
            const mpl_path = Path.resolve(pwd, opts.master_pl_name);
            const mpl_dir = Path.dirname(mpl_path);
            if(!Path.resolve(pwd, playlist_name).startsWith(mpl_dir)) {
                throw new Error(`invalid path of master_pl - ${mpl_dir} ${Path.resolve(pwd, playlist_name)}`);
            }
            try {
                const line = `#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="arib",NAME="ARIB",URI="${Path.resolve(pwd, playlist_name).substring(mpl_dir.length+1)}",LANGUAGE="ja"\n`;
                const body = await FileSystem.readFile(mpl_path);
                if(!body.includes(line)) {
                    await FileSystem.appendFile(mpl_path, line);
                }
            } catch {}
        }
        is_first = false;
        try {
            await Promise.all(rmq.map(x=>FileSystem.rm(x)));
        } catch {}
        write_segment_to = setTimeout(write_segment, opts.hls_time * 1000);
    } catch(ex) {
        console.error('[aribhls]', ex);
        process.exit(-1);
    }
};
write_segment_to = setTimeout(write_segment, opts.hls_time * 1000);

rl.on('line', line => {
    events.push(new AssDialogue(line, eventFormat));
});
