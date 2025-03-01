import numpy as np
import torch
import matplotlib.pyplot as plt
import torch.distributed as dist
plt.switch_backend('agg')


def adjust_learning_rate(optimizer, epoch, args):
    if args.lradj == 'type1':
        lr_adjust = {epoch: args.learning_rate * (0.1 ** epoch)}
    elif args.lradj == 'type2':
        lr_adjust = {epoch: args.learning_rate * (0.5 ** epoch)}
    elif args.lradj == 'type3':
        lr_adjust = {epoch: args.learning_rate * (0.9 ** epoch)}


    if epoch in lr_adjust.keys():
        lr = lr_adjust[epoch]
        for param_group in optimizer.param_groups:
            param_group['lr'] = lr
        print('Updating learning rate to {}'.format(lr))


class EarlyStopping:
    def __init__(self, args, verbose=False, delta=0):
        self.patience = args.patience
        self.verbose = verbose
        self.counter = 0
        self.best_score = None
        self.early_stop = False
        self.val_loss_min = np.inf
        self.delta = delta
        self.dp = args.dp
        self.ddp = args.ddp
        if self.ddp:
            self.local_rank = args.local_rank
        else:
            self.local_rank = None

    def __call__(self, val_loss, model, path):
        score = -val_loss
        if self.best_score is None:
            self.best_score = score
            if self.verbose:
                print(
                    f'Validation loss decreased ({self.val_loss_min:.6f} --> {val_loss:.6f}).')
            self.val_loss_min = val_loss
            if self.ddp:
                if self.local_rank == 0:
                    self.save_checkpoint(val_loss, model, path)
                dist.barrier()
            else:
                self.save_checkpoint(val_loss, model, path)
        elif score < self.best_score + self.delta:
            self.counter += 1
            print(
                f'EarlyStopping counter: {self.counter} out of {self.patience}')
            if self.counter >= self.patience:
                self.early_stop = True
        else:
            self.best_score = score
            if self.ddp:
                if self.local_rank == 0:
                    self.save_checkpoint(val_loss, model, path)
                dist.barrier()
            else:
                self.save_checkpoint(val_loss, model, path)
            if self.verbose:
                print(
                    f'Validation loss decreased ({self.val_loss_min:.6f} --> {val_loss:.6f}).')
            self.val_loss_min = val_loss
            self.counter = 0

    def save_checkpoint(self, val_loss, model, path):
        if self.dp:
            model = model.module
        param_grad_dic = {
            k: v.requires_grad for (k, v) in model.named_parameters()
        }
        state_dict = model.state_dict()
        for k in list(state_dict.keys()):
            if k in param_grad_dic.keys() and not param_grad_dic[k]:
                # delete parameters that do not require gradient
                del state_dict[k]
        torch.save(state_dict, path + '/' + f'checkpoint.pth')

def visual(true, preds=None, name='./pic/test.pdf'):
    """
    Results visualization
    """
    plt.figure()
    plt.plot(true, label='GroundTruth', linewidth=2)
    if preds is not None:
        plt.plot(preds, label='Prediction', linewidth=2)
    plt.legend()
    plt.savefig(name, bbox_inches='tight')