package org.apache.ratis.inst.message;

public class MetaMessage implements Loggable {

    String content;

    public static final MetaMessage RECOVERY_START_MD = new MetaMessage("RECOVERY START");
    public static final MetaMessage RECOVERY_END_MD = new MetaMessage("RECOVERY END");
    public static final MetaMessage FI_ACTIVATED_MD = new MetaMessage("FAILURE INJECTOR ACTIVATED");
    public static final MetaMessage FI_DEACTIVATED_MD = new MetaMessage("FAILURE INJECTOR DEACTIVATED");

    public MetaMessage(String content) {
        this.content = content;
    }

    @Override
    public String logContent() {
       if(content != null) return content;
       else return "";
    }
}
